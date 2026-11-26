import asyncio
import json
import logging
import os
import sqlite3
import time
import weakref
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite

from .config import settings

log = logging.getLogger(__name__)


async def _finish_cleanup(work: Awaitable[None]) -> None:
    # SQLite keeps executing queued work after its caller is cancelled. Drain
    # cleanup before releasing ownership, even if shutdown cancels us again.
    task = asyncio.ensure_future(work)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError

_SCHEMA = """
CREATE TABLE IF NOT EXISTS search_cache (
    cache_key TEXT PRIMARY KEY,
    query     TEXT NOT NULL,
    engines   TEXT NOT NULL,
    results   TEXT NOT NULL,
    created   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pages (
    url      TEXT PRIMARY KEY,
    title    TEXT,
    content  TEXT NOT NULL,
    fetched  INTEGER NOT NULL
);

-- Maintenance (TTL purge + oldest-first eviction) filters and sorts on the
-- timestamps; without these, every pass full-scans the blob-heavy tables.
CREATE INDEX IF NOT EXISTS pages_fetched_idx ON pages(fetched);
CREATE INDEX IF NOT EXISTS search_cache_created_idx ON search_cache(created);

CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
    url UNINDEXED,
    title,
    content,
    content='pages',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
    INSERT INTO pages_fts(rowid, url, title, content)
    VALUES (new.rowid, new.url, new.title, new.content);
END;

CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
    INSERT INTO pages_fts(pages_fts, rowid, url, title, content)
    VALUES ('delete', old.rowid, old.url, old.title, old.content);
END;

CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
    INSERT INTO pages_fts(pages_fts, rowid, url, title, content)
    VALUES ('delete', old.rowid, old.url, old.title, old.content);
    INSERT INTO pages_fts(rowid, url, title, content)
    VALUES (new.rowid, new.url, new.title, new.content);
END;
"""


# A malformed MATCH query is the caller's input and must never trigger a repair.
# Everything else that SQLite can raise here is treated as a possible index
# desync, because the wording of that one is not portable: external-content
# FTS5 reads column values back from `pages` by rowid, and a posting for a row
# that is gone surfaces as `fts5: missing row N from content table …` on
# SQLite 3.42+ but as a bare SQLITE_CORRUPT_VTAB ("database disk image is
# malformed") on older builds — which is what the distro SQLite behind some
# CI interpreters still reports. Matching on the corruption wording therefore
# fails silently on exactly the installs most likely to hold a poisoned cache.
# Classifying the safe-to-ignore case instead is version-independent, and the
# cost of being wrong is one bounded rebuild.
_BAD_MATCH_MARKERS = (
    "syntax error",
    "no such column",
    "unknown special query",
    "unterminated string",
)


def _is_bad_match_query(exc: BaseException) -> bool:
    """True when the MATCH expression itself is invalid, e.g. `a AND` or `x:1`."""
    text = str(exc).lower()
    return any(marker in text for marker in _BAD_MATCH_MARKERS)


# Bumped when an existing cache FILE needs work that `CREATE TABLE IF NOT
# EXISTS` cannot do. 1 = rebuild `pages_fts`, whose index was corrupted by the
# `INSERT OR REPLACE` in the old `put_page` (see the comment there).
_USER_VERSION = 1


class Cache:
    # One owned background pass on open and after each batch of writes.
    _MAINTAIN_EVERY = 200
    # Commit between bounded FTS merge steps so other writers can proceed.
    _FTS_MERGE_PAGES = 64

    def __init__(self) -> None:
        self._path = str(settings.cache_path())
        self._conn_obj: aiosqlite.Connection | None = None
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
        self._writes_since_maintain = 0
        # Reference to the in-flight background maintenance task (if any) so
        # it isn't garbage-collected mid-run and never overlaps itself.
        self._maintain_task: asyncio.Task | None = None

    def _get_lock(self, name: str) -> asyncio.Lock:
        # Holders and waiters retain their lock; the idle cache must not retain
        # a contended lock's event loop after close and reopen on another loop.
        lock = self._locks.get(name)
        if lock is None:
            lock = self._locks[name] = asyncio.Lock()
        return lock

    @asynccontextmanager
    async def _operation(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._get_lock("operation"):
            conn = await self._conn()
            try:
                yield conn
            except BaseException:
                # A failed or cancelled statement can leave an implicit transaction
                # open; drain its rollback before the next operation acquires the lock.
                await _finish_cleanup(conn.rollback())
                raise

    @asynccontextmanager
    async def _write_operation(self) -> AsyncIterator[aiosqlite.Connection]:
        # Wait for local maintenance before taking the foreground connection:
        # SQLite's busy handler would otherwise stall reads and time out writes.
        async with self._get_lock("writer"), self._operation() as conn:
            yield conn

    async def _conn(self) -> aiosqlite.Connection:
        """Return the single long-lived connection, creating it once.

        The connection is opened (a single background thread) and the schema +
        pragmas are applied exactly once, under a lock so that concurrent first
        callers don't race the initialization or end up with two connections.
        """
        if self._conn_obj is not None:
            return self._conn_obj
        async with self._get_lock("connection"):
            # Re-check inside the lock: another coroutine may have initialized
            # the connection while we were waiting to acquire it.
            if self._conn_obj is not None:
                return self._conn_obj
            conn = aiosqlite.connect(self._path)
            # aiosqlite drives SQLite on a private, non-daemon worker thread.
            # This connection is long-lived and may never be explicitly closed
            # (interpreter exit, or a test runner reusing the module singleton
            # across event loops), and a live non-daemon thread blocks process
            # shutdown forever. Mark the worker daemon BEFORE it starts so a
            # missing close() can never hang exit. The cache holds disposable
            # data: with WAL + synchronous=NORMAL an abrupt exit can lose the
            # last few commits but never corrupts the file.
            worker = getattr(conn, "_thread", None)
            if worker is not None:
                worker.daemon = True
            conn = await conn
            try:
                # WAL lets readers and a writer proceed concurrently; the
                # busy_timeout makes a contended writer wait instead of
                # immediately raising 'database is locked'.
                await conn.execute("PRAGMA journal_mode=WAL")
                await conn.execute("PRAGMA busy_timeout=5000")
                # NORMAL is the documented safe default under WAL: fsync at
                # checkpoints instead of every commit, so each put_page /
                # put_search stops paying a per-write fsync.
                await conn.execute("PRAGMA synchronous=NORMAL")
                await conn.execute("PRAGMA temp_store=MEMORY")
                await conn.executescript(_SCHEMA)
                await self._migrate(conn)
                await conn.commit()
            except BaseException:
                await conn.close()
                raise
            self._conn_obj = conn
            # A separate SQLite worker keeps maintenance out of the read queue
            # and prevents foreground cursors from blocking VACUUM on this connection.
            self._spawn_maintenance()
            return conn

    def _spawn_maintenance(self) -> None:
        """Schedule at most one pass, owned and drained by close()."""
        if self._maintain_task is not None and not self._maintain_task.done():
            return
        self._maintain_task = asyncio.get_running_loop().create_task(
            self._background_maintenance()
        )

    async def _background_maintenance(self) -> None:
        async with self._get_lock("writer"):
            conn = aiosqlite.connect(self._path)
            worker = getattr(conn, "_thread", None)
            if worker is not None:
                worker.daemon = True
            opening = asyncio.ensure_future(conn)
            try:
                # Cancelling aiosqlite's opener stops waiting, not its worker. Keep
                # ownership until acquisition settles so close() can drain it.
                await asyncio.shield(opening)
                # Other processes may still contend for SQLite's writer lock.
                for statement in (
                    "PRAGMA busy_timeout=1000",
                    "PRAGMA synchronous=NORMAL",
                    "PRAGMA temp_store=MEMORY",
                ):
                    async with conn.execute(statement):
                        pass
                await self._maintain(conn)
            except Exception as exc:
                log.warning("cache maintenance connection failed: %s", exc)
            finally:
                async def release() -> None:
                    try:
                        await opening
                    except Exception:
                        return  # aiosqlite stops its worker when opening fails.
                    try:
                        await conn.interrupt()
                    finally:
                        await conn.close()

                await _finish_cleanup(release())

    @classmethod
    async def _compact_fts(cls, conn: aiosqlite.Connection) -> None:
        # Start one merge across levels; positive steps continue it without
        # restarting when another connection adds segments in between commits.
        pages = -cls._FTS_MERGE_PAGES
        while True:
            before = conn.total_changes
            async with conn.execute(
                "INSERT INTO pages_fts(pages_fts, rank) VALUES('merge', ?)", (pages,),
            ):
                pass
            await conn.commit()
            if conn.total_changes - before < 2:
                return
            pages = cls._FTS_MERGE_PAGES

    def _db_size(self) -> int:
        """Current on-disk footprint: main db file + WAL (best-effort)."""
        size = 0
        for suffix in ("", "-wal"):
            try:
                size += os.path.getsize(self._path + suffix)
            except OSError:
                pass
        return size

    async def _maintain(self, conn: aiosqlite.Connection) -> None:
        """Purge expired rows, then enforce the size cap. Never raises.

        Count live SQLite pages, not WAL history, when evicting. Recheck after
        each batch because row sizes vary; search-only clients can fill the
        database too. Vacuum once, and only if there are free pages to reclaim.
        """
        try:
            cutoff = int(time.time()) - settings.cache_ttl_seconds
            async with conn.execute("DELETE FROM search_cache WHERE created < ?", (cutoff,)):
                pass
            async with conn.execute("DELETE FROM pages WHERE fetched < ?", (cutoff,)):
                pass
            await conn.commit()

            cap = settings.cache_max_mb * 1024 * 1024
            size = self._db_size()
            if cap <= 0 or size <= cap:
                return
            async def storage() -> tuple[int, int, int]:
                async with conn.execute(
                    "SELECT page_count, freelist_count, page_size FROM "
                    "pragma_page_count(), pragma_freelist_count(), pragma_page_size()"
                ) as cur:
                    return tuple(await cur.fetchone())

            pages, free, page_size = await storage()
            live_size = (pages - free) * page_size
            if live_size > cap:
                # FTS deletion history is allocated storage, not useful content.
                # Finish reclaiming it before deciding to evict valid pages.
                await self._compact_fts(conn)
                pages, free, page_size = await storage()
                live_size = (pages - free) * page_size
            while live_size > cap:
                dropped = 0
                # Index-backed oldest-first batches; never scan result blobs.
                # Rechecking live pages handles a few large *new* rows mixed
                # with many small old ones, without repeated full VACUUMs.
                for table, timestamp in (("pages", "fetched"), ("search_cache", "created")):
                    async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                        total = (await cur.fetchone())[0]
                    if total:
                        keep = int(total * (cap * 0.8) / live_size)
                        drop = max(1, total - keep)
                        async with conn.execute(
                            f"DELETE FROM {table} WHERE rowid IN "
                            f"(SELECT rowid FROM {table} "
                            f"ORDER BY {timestamp}, rowid LIMIT ?)",
                            (drop,),
                        ) as cur:
                            dropped += cur.rowcount
                        await conn.commit()
                        if table == "pages":
                            await self._compact_fts(conn)
                        pages, free, page_size = await storage()
                        live_size = (pages - free) * page_size
                        if live_size <= cap:
                            break
                if not dropped:  # The schema/FTS floor cannot be evicted.
                    break
            if free and pages * page_size > cap:
                async with conn.execute("VACUUM"):
                    pass
            # A reader may retain a WAL snapshot; defer reclamation rather than
            # mistake that history for live content and evict additional rows.
            async with conn.execute("PRAGMA wal_checkpoint(TRUNCATE)") as cur:
                checkpoint = await cur.fetchone()
            if checkpoint and checkpoint[0]:
                log.info("cache checkpoint deferred: another connection retains a WAL snapshot")
        except Exception as e:
            # Maintenance is best-effort housekeeping — a failure (locked db,
            # disk error) must never take down the actual read/write path.
            log.warning("cache maintenance failed: %s", e)

    async def _bump_writes(self) -> None:
        self._writes_since_maintain += 1
        if self._writes_since_maintain >= self._MAINTAIN_EVERY:
            self._writes_since_maintain = 0
            self._spawn_maintenance()

    async def close(self) -> None:
        """Close the long-lived connection, if any. Safe to call repeatedly."""
        async def release() -> None:
            # Waiting for an active read is part of shutdown ownership too.
            async with self._get_lock("operation"), self._get_lock("connection"):
                if self._maintain_task is not None and not self._maintain_task.done():
                    self._maintain_task.cancel()
                    try:
                        await self._maintain_task
                    except (asyncio.CancelledError, Exception):
                        pass
                self._maintain_task = None
                if self._conn_obj is not None:
                    await self._conn_obj.close()
                    self._conn_obj = None

        await _finish_cleanup(release())

    @staticmethod
    async def _migrate(conn: aiosqlite.Connection) -> None:
        """Additive column migrations. Idempotent, cheap, never destructive.

        `CREATE TABLE IF NOT EXISTS` cannot add a column to a table that already
        exists, so a schema that grew after a user's cache file was created has
        to be reconciled here. SQLite's `ADD COLUMN` is O(1) and backfills NULL,
        which is the correct value for every pre-existing row: we genuinely do
        not know the provenance of a result cached before we started recording
        it.
        """
        # Independent server processes share this file. Lock before reading
        # schema state, not just before ALTER, so only one migrates/rebuilds.
        await conn.execute("BEGIN IMMEDIATE")
        cur = await conn.execute("PRAGMA table_info(search_cache)")
        columns = {row[1] for row in await cur.fetchall()}
        if "meta" not in columns:
            await conn.execute("ALTER TABLE search_cache ADD COLUMN meta TEXT")

        # One-time repair of caches written before `put_page` stopped using
        # `INSERT OR REPLACE`. Those files carry orphaned `pages_fts` postings
        # that make every `cache_search` raise, so the fix above only helps
        # rows written from now on — the existing index still has to be
        # rebuilt. `user_version` gates it to exactly once per file; the
        # rebuild is O(corpus) and must not run on every start.
        cur = await conn.execute("PRAGMA user_version")
        row = await cur.fetchone()
        if (row[0] if row else 0) < _USER_VERSION:
            await conn.execute("INSERT INTO pages_fts(pages_fts) VALUES('rebuild')")
            await conn.execute(f"PRAGMA user_version={_USER_VERSION}")

    async def get_search(
        self, key: str, max_age_seconds: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
        """Cached results plus the provenance recorded alongside them.

        Returns `(results, meta)`; `meta` is `{}` for rows written before the
        `meta` column existed. Provenance is part of the ANSWER, not a
        statistic about the run that produced it: a result set that only exists
        because a captcha-walled engine was rescued by a reserve must still say so
        on the cache-hit path, or the second identical query silently
        re-labels a recovered result set as a normal one.
        """
        async with self._operation() as conn:
            rows = await conn.execute_fetchall(
                "SELECT results, created, meta FROM search_cache WHERE cache_key=?", (key,),
            )
        if not rows:
            return None
        row = rows[0]
        ttl = max_age_seconds if max_age_seconds is not None else settings.cache_ttl_seconds
        if time.time() - row[1] > ttl:
            return None
        meta: dict[str, Any] = {}
        if row[2]:
            try:
                loaded = json.loads(row[2])
            except ValueError:
                loaded = None
            if isinstance(loaded, dict):
                meta = loaded
        # When the row was written — so a hit can say how old it is. Derived
        # from the row on every read, never stored inside `meta`.
        meta["cached_at"] = row[1]
        return json.loads(row[0]), meta

    async def put_search(
        self,
        key: str,
        query: str,
        engines: list[str],
        results: list[dict[str, Any]],
        meta: dict[str, Any] | None = None,
        *,
        created: int | None = None,
    ) -> None:
        """Write a search's row. `created` keeps an earlier write's time, for a
        rewrite that adds to results retrieved then (the agent lane's late
        merge) and must not make them look newer."""
        async with self._write_operation() as conn:
            async with conn.execute(
                "INSERT OR REPLACE INTO search_cache "
                "(cache_key, query, engines, results, created, meta) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    key,
                    query,
                    ",".join(engines),
                    json.dumps(results, ensure_ascii=False),
                    int(time.time()) if created is None else created,
                    json.dumps(meta, ensure_ascii=False) if meta else None,
                ),
            ):
                pass
            await conn.commit()
            await self._bump_writes()

    async def get_page(
        self, url: str, max_age_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        async with self._operation() as conn:
            rows = await conn.execute_fetchall(
                "SELECT title, content, fetched FROM pages WHERE url=?", (url,),
            )
        if not rows:
            return None
        row = rows[0]
        ttl = max_age_seconds if max_age_seconds is not None else settings.cache_ttl_seconds
        if time.time() - row[2] > ttl:
            return None
        return {"url": url, "title": row[0], "content": row[1], "fetched": row[2]}

    async def put_page(self, url: str, title: str | None, content: str) -> None:
        # UPSERT preserves the rowid and fires pages_au. REPLACE silently deletes
        # the old row without pages_ad unless recursive_triggers is enabled,
        # leaving orphaned FTS postings that break later full-text searches.
        async with self._write_operation() as conn:
            async with conn.execute(
                "INSERT INTO pages (url, title, content, fetched) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(url) DO UPDATE SET "
                "title=excluded.title, content=excluded.content, fetched=excluded.fetched",
                (url, title or "", content, int(time.time())),
            ):
                pass
            await conn.commit()
            await self._bump_writes()

    async def complete_page_urls(self, fragment: str, limit: int = 50) -> list[str]:
        """Cached page URLs containing `fragment`, most recently fetched first.

        For MCP completion of `cache://page/{url}`. LIKE with the wildcards
        escaped: a fragment is text somebody typed, not a pattern.
        """
        needle = fragment.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        async with self._operation() as conn:
            rows = await conn.execute_fetchall(
                "SELECT url FROM pages WHERE url LIKE ? ESCAPE '\\' ORDER BY fetched DESC LIMIT ?",
                (f"%{needle}%", limit),
            )
        return [row[0] for row in rows]

    async def recent_search_keys(self, prefix: str = "", limit: int = 50) -> list[str]:
        """Cache keys of stored searches starting with `prefix`, newest first."""
        async with self._operation() as conn:
            rows = await conn.execute_fetchall(
                "SELECT cache_key FROM search_cache WHERE cache_key LIKE ? "
                "ORDER BY created DESC LIMIT ?",
                (f"{prefix}%", limit),
            )
        return [row[0] for row in rows]

    async def search_pages(
        self, query: str, limit: int = 10, *, _retrying: bool = False
    ) -> list[dict[str, Any]]:
        try:
            async with self._operation() as conn:
                rows = await conn.execute_fetchall(
                    "SELECT url, title, snippet(pages_fts, 2, '[', ']', '...', 16) "
                    "FROM pages_fts WHERE pages_fts MATCH ? "
                    "ORDER BY bm25(pages_fts) LIMIT ?",
                    (query, limit),
                )
        except (sqlite3.OperationalError, aiosqlite.Error) as exc:
            if not _retrying and not _is_bad_match_query(exc):
                # Orphaned external-content postings need one rebuild, not a
                # permanently empty result. Release the operation before retrying.
                log.warning("%s: rebuilding FTS index (%s)", self._path, exc)
                try:
                    async with self._write_operation() as conn:
                        async with conn.execute(
                            "INSERT INTO pages_fts(pages_fts) VALUES('rebuild')",
                        ):
                            pass
                        await conn.commit()
                except (sqlite3.OperationalError, aiosqlite.Error) as rebuild_exc:
                    log.warning("%s: FTS rebuild failed (%s)", self._path, rebuild_exc)
                    return []
                return await self.search_pages(query, limit, _retrying=True)
            # Malformed MATCH input remains "no matches"; callers add a friendly hint.
            return []
        return [{"url": r[0], "title": r[1], "snippet": r[2]} for r in rows]


cache = Cache()

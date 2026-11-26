"""Offline performance/lifecycle checks beyond the audit's runtime regressions."""
from __future__ import annotations

import asyncio
import gc
import sqlite3
import threading
import time
import weakref

import aiosqlite
import anyio
import pytest

from search_mcp import browser, browser_profile, oauth, server
from search_mcp.cache import Cache
from search_mcp.config import settings


@pytest.fixture
async def cache(monkeypatch):
    monkeypatch.setattr(settings, "cache_max_mb", 1)
    monkeypatch.setattr(settings, "cache_ttl_seconds", 10**9)
    instance = Cache()
    conn = await instance._conn()
    await instance._maintain_task
    try:
        yield instance, conn
    finally:
        await instance.close()


async def test_skewed_search_rows_fit_cap_and_keep_newest(cache):
    instance, conn = cache
    now = int(time.time())
    await conn.executemany(
        "INSERT INTO search_cache(cache_key, query, engines, results, created) "
        "VALUES (?, 'q', 'fake', ?, ?)",
        [(str(i), "x" * (1000 if i < 25 else 250_000), now - 30 + i) for i in range(30)],
    )
    await conn.commit()
    statements = []
    await conn.set_trace_callback(statements.append)
    await instance._maintain(conn)
    assert instance._db_size() <= 1024 * 1024
    async with conn.execute("SELECT cache_key FROM search_cache ORDER BY created") as cur:
        keys = [int(row[0]) for row in await cur.fetchall()]
    assert keys and keys == list(range(keys[0], 30))
    vacuums = statements.count("VACUUM")
    assert vacuums <= 1
    await instance._maintain(conn)
    assert statements.count("VACUUM") == vacuums


async def test_mixed_cache_keeps_newest_in_both_tables(cache):
    instance, conn = cache
    now = int(time.time())
    for table, key, stamp in (("pages", "url", "fetched"),
                              ("search_cache", "cache_key", "created")):
        if table == "pages":
            sql = f"INSERT INTO {table}({key}, content, {stamp}) VALUES (?, ?, ?)"
        else:
            sql = (f"INSERT INTO {table}({key}, results, {stamp}, query, engines) "
                   "VALUES (?, ?, ?, 'q', 'fake')")
        await conn.executemany(sql, [(str(i), "x" * 50_000, now - 30 + i) for i in range(30)])
    await conn.commit()
    await instance._maintain(conn)
    assert instance._db_size() <= 1024 * 1024
    for table, key, stamp in (("pages", "url", "fetched"),
                              ("search_cache", "cache_key", "created")):
        async with conn.execute(f"SELECT {key} FROM {table} ORDER BY {stamp}") as cur:
            keys = [int(row[0]) for row in await cur.fetchall()]
        assert 0 < len(keys) < 30
        assert keys == list(range(keys[0], 30))


async def test_page_eviction_stops_before_deleting_a_small_search(cache):
    instance, conn = cache
    now = int(time.time())
    await conn.executemany(
        "INSERT INTO pages(url, content, fetched) VALUES (?, ?, ?)",
        [(str(i), "x" * 600_000, now - 2 + i) for i in range(2)],
    )
    await conn.commit()
    await instance.put_search("newest", "q", ["fake"], [{"snippet": "small"}])
    assert instance._db_size() > 1024 * 1024
    await instance._maintain(conn)
    assert instance._db_size() <= 1024 * 1024
    assert await instance.get_search("newest") is not None
    async with conn.execute("SELECT url FROM pages") as cur:
        assert await cur.fetchall() == [("1",)]


async def test_wal_growth_alone_never_evicts_or_vacuums(cache):
    instance, conn = cache
    await conn.execute("PRAGMA wal_autocheckpoint=0")
    for _ in range(20):
        await instance.put_search("same", "q", ["fake"], [{"snippet": "x" * 80_000}])
    assert instance._db_size() > 1024 * 1024
    statements = []
    await conn.set_trace_callback(statements.append)
    await instance._maintain(conn)
    assert await instance.get_search("same") is not None
    assert instance._db_size() <= 1024 * 1024
    assert "VACUUM" not in statements


async def test_no_reclaimable_pages_means_no_repeated_vacuum(cache, monkeypatch):
    instance, conn = cache
    # An untruncatable WAL (e.g. another reader) is not reclaimable DB storage.
    monkeypatch.setattr(instance, "_db_size", lambda: 2 * 1024 * 1024)
    statements = []
    await conn.set_trace_callback(statements.append)
    await instance._maintain(conn)
    await instance._maintain(conn)
    assert "VACUUM" not in statements


async def test_background_cache_sql_does_not_block_foreground_reads(cache, monkeypatch):
    instance, foreground = cache
    await instance.put_page("https://example.test/newest", "Newest", "retained content")
    entered = threading.Event()
    release = threading.Event()
    connections = []

    def pause():
        entered.set()
        if not release.wait(5):
            raise RuntimeError("test did not release maintenance")
        return 1

    async def maintenance(conn):
        connections.append(conn)
        await conn.create_function("maintenance_pause", 0, pause)
        await conn.execute_fetchall("SELECT maintenance_pause()")

    monkeypatch.setattr(instance, "_maintain", maintenance)
    instance._spawn_maintenance()
    task = instance._maintain_task
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert connections == [connections[0]] and connections[0] is not foreground
        page = await asyncio.wait_for(instance.get_page("https://example.test/newest"), 2)
        assert page["content"] == "retained content"
        instance._spawn_maintenance()
        assert instance._maintain_task is task
    finally:
        release.set()
        await asyncio.wait_for(task, 5)
    with pytest.raises(ValueError, match="no active connection"):
        await connections[0].execute("SELECT 1")


@pytest.mark.parametrize("write_kind", ["page", "search"])
async def test_maintenance_writer_wait_does_not_timeout_or_block_reads(cache, monkeypatch, write_kind):
    instance, foreground = cache
    url = "https://example.test/retained"
    await instance.put_page(url, "Title", "retained content")
    async with foreground.execute("PRAGMA busy_timeout=20"):
        pass
    entered, release = asyncio.Event(), asyncio.Event()

    async def maintenance(conn):
        async with conn.execute("UPDATE pages SET title=title WHERE url=?", (url,)):
            pass
        entered.set()
        await release.wait()
        await conn.commit()

    async def write():
        if write_kind == "page":
            await instance.put_page("https://example.test/written", "Title", "new")
        else:
            await instance.put_search("written", "q", ["fake"], [{"url": url}])

    monkeypatch.setattr(instance, "_maintain", maintenance)
    instance._spawn_maintenance()
    writing = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        writing = asyncio.create_task(write())
        done, _ = await asyncio.wait({writing}, timeout=0.1)
        assert not done, "local maintenance exhausted the foreground busy timeout"
        page = await asyncio.wait_for(instance.get_page(url), 1)
        assert page["content"] == "retained content"
        release.set()
        await asyncio.wait_for(writing, 5)
        assert not foreground.in_transaction
    finally:
        release.set()
        if writing is not None:
            await asyncio.gather(writing, return_exceptions=True)
        await asyncio.wait_for(instance._maintain_task, 5)


@pytest.mark.parametrize("direct_connection", [False, True])
def test_closed_cache_does_not_retain_retired_event_loops(tmp_path, monkeypatch, direct_connection):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    instance = Cache()
    loops = []

    async def use_and_close():
        loops.append(weakref.ref(asyncio.get_running_loop()))
        try:
            if direct_connection:
                await asyncio.gather(*(instance._conn() for _ in range(4)))
            await asyncio.gather(*(
                instance.put_page(f"https://example.test/{index}", "Title", "content")
                for index in range(4)
            ))
            assert (await instance.get_page("https://example.test/3"))["content"] == "content"
        finally:
            await instance.close()

    for _ in range(3):
        asyncio.run(use_and_close())
    gc.collect()
    assert all(loop() is None for loop in loops)


@pytest.mark.parametrize("close_cache", [False, True], ids=["cancel-writer", "close-cache"])
async def test_maintenance_waiting_writer_cancellation_releases_ownership(cache, monkeypatch, close_cache):
    instance, foreground = cache
    url = "https://example.test/retained"
    await instance.put_page(url, "Title", "retained content")
    entered, release = asyncio.Event(), asyncio.Event()

    async def maintenance(conn):
        async with conn.execute("UPDATE pages SET title=title WHERE url=?", (url,)):
            pass
        entered.set()
        await release.wait()
        await conn.commit()

    monkeypatch.setattr(instance, "_maintain", maintenance)
    instance._spawn_maintenance()
    task = instance._maintain_task
    writing = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        writing = asyncio.create_task(instance.put_page("https://example.test/new", "Title", "new"))
        done, _ = await asyncio.wait({writing}, timeout=0.05)
        assert not done
        if close_cache:
            # Shutdown must cancel maintenance even with a foreground writer waiting.
            await asyncio.wait_for(instance.close(), 1)
            release.set()
            await asyncio.wait_for(writing, 5)
            assert task.cancelled()
        else:
            writing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(writing, 1)
            assert not task.done(), "cancelling a waiting writer cancelled maintenance"
            page = await asyncio.wait_for(instance.get_page(url), 1)
            assert page["content"] == "retained content"
            release.set()
            await asyncio.wait_for(task, 5)
            assert await instance.get_page("https://example.test/new") is None
            assert not foreground.in_transaction
        await instance.put_page("https://example.test/later", "Title", "later")
        assert (await instance.get_page("https://example.test/later"))["content"] == "later"
    finally:
        release.set()
        if writing is not None:
            await asyncio.gather(writing, return_exceptions=True)
        await instance.close()


async def test_background_maintenance_defers_checkpoint_for_a_held_reader(cache, caplog):
    instance, foreground = cache
    now = int(time.time())
    async with foreground.executemany(
        "INSERT INTO pages(url, content, fetched) VALUES (?, ?, ?)",
        [(f"https://example.test/{i}", "x" * 50_000, now - 30 + i) for i in range(30)],
    ):
        pass
    await foreground.commit()
    caplog.set_level("INFO", logger="search_mcp.cache")

    async with foreground.execute("SELECT url FROM pages ORDER BY fetched") as reader:
        assert await reader.fetchone() == ("https://example.test/0",)
        instance._spawn_maintenance()
        await asyncio.wait_for(instance._maintain_task, 5)
        assert "cache maintenance failed" not in caplog.text
        assert "cache maintenance connection failed" not in caplog.text
        assert "cache checkpoint deferred" in caplog.text
        assert len(await reader.fetchall()) == 29

    instance._spawn_maintenance()
    await asyncio.wait_for(instance._maintain_task, 5)
    assert instance._db_size() <= 1024 * 1024
    assert await instance.get_page("https://example.test/29") is not None
    assert "cache maintenance failed" not in caplog.text


@pytest.mark.parametrize("write_kind", ["page", "search"])
async def test_public_read_and_maintenance_do_not_poison_later_writes(
    tmp_path, monkeypatch, write_kind,
):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    monkeypatch.setattr(settings, "cache_max_mb", 0)
    monkeypatch.setattr(settings, "cache_ttl_seconds", 60)
    entered, release, armed = threading.Event(), threading.Event(), threading.Event()
    real_connect = aiosqlite.connect

    class SnapshotConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            cursor = super().execute(sql, parameters)
            if armed.is_set() and sql.startswith("SELECT url FROM pages WHERE url LIKE"):
                armed.clear()
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test did not release the foreground read")
            return cursor

    def connect(*args, **kwargs):
        return real_connect(*args, factory=SnapshotConnection, **kwargs)

    monkeypatch.setattr(aiosqlite, "connect", connect)
    instance = Cache()
    tasks = []
    try:
        foreground = await instance._conn()
        await instance._maintain_task
        for index in range(2):
            await instance.put_page(f"https://example.test/{index}", "Title", "fresh")
        async with foreground.execute(
            "INSERT INTO pages(url, content, fetched) VALUES (?, ?, ?)",
            ("https://example.test/expired", "old", int(time.time()) - 1000),
        ):
            pass
        await foreground.commit()
        armed.set()
        reading = asyncio.create_task(instance.complete_page_urls("example.test/"))
        tasks.append(reading)
        assert await asyncio.to_thread(entered.wait, 5)

        # Advance WAL while the public read owns an unfinished snapshot.
        instance._spawn_maintenance()
        await asyncio.wait_for(instance._maintain_task, 5)
        write_started = asyncio.Event()

        async def write():
            write_started.set()
            if write_kind == "page":
                await instance.put_page("https://example.test/written", "Title", "new")
            else:
                await instance.put_search("written", "q", ["fake"], [{"url": "https://example.test/"}])

        writing = asyncio.create_task(write())
        tasks.append(writing)
        await write_started.wait()
        release.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        assert isinstance(outcomes[0], list), outcomes
        assert outcomes[1] is None, outcomes
        assert not foreground.in_transaction
        await instance.put_page("https://example.test/later", "Title", "later")
        await instance.put_search("later", "q", ["fake"], [{"url": "https://example.test/later"}])
        assert await instance.get_page("https://example.test/later") is not None
        assert await instance.get_search("later") is not None
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await instance.close()


@pytest.mark.parametrize("cancellations", [0, 1, 3])
async def test_close_drains_foreground_read_before_propagating_cancellation(
    tmp_path, monkeypatch, cancellations,
):
    monkeypatch.setattr(settings, "cache_dir", tmp_path)
    entered, release, armed = threading.Event(), threading.Event(), threading.Event()
    real_connect = aiosqlite.connect

    class GatedConnection(sqlite3.Connection):
        def execute(self, sql, parameters=(), /):
            cursor = super().execute(sql, parameters)
            if armed.is_set() and sql.startswith("SELECT title, content, fetched FROM pages"):
                armed.clear()
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test did not release the foreground read")
            return cursor

    def connect(*args, **kwargs):
        return real_connect(*args, factory=GatedConnection, **kwargs)

    monkeypatch.setattr(aiosqlite, "connect", connect)
    instance = Cache()
    tasks = []
    url = "https://example.test/retained"
    try:
        await instance.put_page(url, "Title", "retained content")
        await asyncio.wait_for(instance._maintain_task, 5)
        foreground = instance._conn_obj
        armed.set()
        reading = asyncio.create_task(instance.get_page(url))
        tasks.append(reading)
        assert await asyncio.to_thread(entered.wait, 5)
        closing = asyncio.create_task(instance.close())
        tasks.append(closing)
        done, _ = await asyncio.wait({closing}, timeout=0.05)
        assert not done, "close returned before the foreground read completed"
        for _ in range(cancellations):
            closing.cancel()
            done, _ = await asyncio.wait({closing}, timeout=0.05)
            assert not done, "cancelled close abandoned the foreground read"

        release.set()
        page = await asyncio.wait_for(reading, 5)
        assert page["url"] == url and page["content"] == "retained content"
        if cancellations:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(closing, 5)
        else:
            await asyncio.wait_for(closing, 5)
        assert instance._conn_obj is None
        assert instance._maintain_task is None
        with pytest.raises(ValueError, match="no active connection"):
            await foreground.execute("SELECT 1")
        worker = getattr(foreground, "_thread", None)
        if worker is not None:
            await asyncio.to_thread(worker.join, 5)
            assert not worker.is_alive()

        assert (await asyncio.wait_for(instance.get_page(url), 5))["content"] == "retained content"
        await asyncio.wait_for(instance.put_page(url, "Updated", "reopened content"), 5)
        assert (await asyncio.wait_for(instance.get_page(url), 5))["content"] == "reopened content"
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
        await asyncio.wait_for(instance.close(), 5)


async def test_close_interrupts_and_closes_running_cache_maintenance(cache, monkeypatch):
    instance, foreground = cache
    entered = threading.Event()
    connections = []

    async def maintenance(conn):
        connections.append(conn)
        deadline = time.monotonic() + 10
        # Bound even a failed interrupt so the regression cannot leave a busy worker.
        await conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        await conn.set_trace_callback(lambda sql: entered.set())
        await conn.execute_fetchall(
            "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL "
            "SELECT x + 1 FROM counter) SELECT sum(x) FROM counter"
        )

    monkeypatch.setattr(instance, "_maintain", maintenance)
    instance._spawn_maintenance()
    task = instance._maintain_task
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert connections[0] is not foreground
        await asyncio.wait_for(instance.close(), 5)
        assert task.cancelled()
        assert instance._maintain_task is None
        assert instance._conn_obj is None
        with pytest.raises(ValueError, match="no active connection"):
            await connections[0].execute("SELECT 1")
    finally:
        if not task.done():
            await connections[0].interrupt()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await instance.close()


@pytest.mark.parametrize("cancel_close", [False, True])
async def test_close_drains_maintenance_connection_acquisition(cache, monkeypatch, cancel_close):
    instance, _ = cache
    entered = threading.Event()
    release = threading.Event()
    connections = []
    real_connect = aiosqlite.connect

    class DelayedConnection(sqlite3.Connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test did not release connection acquisition")

    def connect(*args, **kwargs):
        connection = real_connect(*args, factory=DelayedConnection, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(aiosqlite, "connect", connect)
    instance._spawn_maintenance()
    task = instance._maintain_task
    closing = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        closing = asyncio.create_task(instance.close())
        done, _ = await asyncio.wait({closing}, timeout=0.05)
        assert not done, "close returned while its connection worker was still opening"
        if cancel_close:
            closing.cancel()
            done, _ = await asyncio.wait({closing}, timeout=0.05)
            assert not done, "cancelled close abandoned connection acquisition"
        release.set()
        if cancel_close:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(closing, 5)
        else:
            await asyncio.wait_for(closing, 5)
        assert task.cancelled()
        assert instance._maintain_task is None
        assert instance._conn_obj is None
        with pytest.raises(ValueError, match="no active connection"):
            await connections[0].execute("SELECT 1")
    finally:
        release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await instance.close()
        for connection in connections:
            await connection.close()
            worker = getattr(connection, "_thread", None)
            if worker is not None and worker.ident is not None:
                await asyncio.to_thread(worker.join, 5)


@pytest.mark.parametrize("stage", ["start", "script"])
async def test_cancelled_initialization_closes_owned_resources(monkeypatch, stage):
    entered = asyncio.Event()
    closed = []

    class Context:
        async def add_init_script(self, script):
            entered.set()
            await asyncio.Event().wait()

        async def close(self):
            closed.append("context")

    class Driver:
        async def stop(self):
            closed.append("driver")

    class Manager:
        async def start(self):
            if stage == "start":
                entered.set()
                await asyncio.Event().wait()
            return Driver()

        async def __aexit__(self, *args):
            closed.append("manager")

    async def launch(*args):
        return Context()

    monkeypatch.setattr(browser, "async_playwright", Manager)
    monkeypatch.setattr(browser, "_launch_context", launch)
    pool = browser.BrowserPool()
    task = asyncio.create_task(pool._ensure())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed == (["manager"] if stage == "start" else ["context", "driver"])
        assert pool._ctx is None and pool._playwright is None and pool._lease is None
        assert not pool._lock.locked()
    finally:
        await pool.shutdown()


async def test_cancelled_login_reclaims_private_profile_without_loop_callback(monkeypatch):
    entered, release, reclaimed = threading.Event(), threading.Event(), threading.Event()
    real_acquire = browser_profile.acquire_private
    leases = []

    def acquire(path):
        lease = real_acquire(path)
        leases.append(lease)
        real_release = lease.release

        def reclaim():
            real_release()
            reclaimed.set()

        lease.release = reclaim
        entered.set()
        release.wait(5)
        return lease

    class Manager:
        async def start(self):
            pytest.fail("cancelled profile acquisition must not launch a browser")

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(browser_profile, "acquire_private", acquire)
    monkeypatch.setattr(browser, "async_playwright", Manager)
    task = asyncio.create_task(browser.BrowserPool().login("https://example.invalid"))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        release.set()
        # Intentionally block this loop: reclamation must live in the worker,
        # not in a completion callback that cannot run once asyncio.run exits.
        assert reclaimed.wait(2)
        assert not leases[0].path.exists()
    finally:
        release.set()
        for lease in leases:
            lease.release()


async def test_cancelled_published_lease_finishes_release_under_anyio(monkeypatch):
    released = []
    real_to_thread = asyncio.to_thread

    class Lease:
        def release(self):
            released.append(True)

    monkeypatch.setattr(browser_profile, "acquire", lambda path: Lease())
    with anyio.CancelScope() as scope:
        async def to_thread(work, *args):
            # Deliver cancellation after publication, before the loop receives
            # the result; make cleanup's await an explicit cancellation point.
            if work.__name__ == "work":
                await real_to_thread(work, *args)
                scope.cancel()
            await anyio.lowlevel.checkpoint()
            return await real_to_thread(work, *args)

        monkeypatch.setattr(asyncio, "to_thread", to_thread)
        await browser_profile.acquire_async(settings.cache_dir)
    assert released == [True]


async def test_cancelled_lifespan_still_drains_refreshes_and_closes(monkeypatch):
    closed = []

    async def noop():
        pass

    def cleanup(name):
        async def run():
            await anyio.lowlevel.checkpoint()
            closed.append(name)
        return run

    monkeypatch.setattr(server, "prewarm", noop)
    monkeypatch.setattr(server, "_report_agent_problem", noop)
    monkeypatch.setattr(oauth, "finish_refreshes", cleanup("refreshes"))
    monkeypatch.setattr(server.pool, "shutdown", cleanup("browser"))
    monkeypatch.setattr(server.cache, "close", cleanup("cache"))
    with anyio.CancelScope() as scope:
        async with server._lifespan(server.mcp):
            scope.cancel()
    assert closed == ["refreshes", "browser", "cache"]


def test_run_does_not_create_cleanup_loops(monkeypatch):
    runs, closed = [], []

    async def noop():
        pass

    async def refreshes():
        closed.append(("refreshes", asyncio.get_running_loop()))

    async def shutdown():
        closed.append(("browser", asyncio.get_running_loop()))

    async def close():
        closed.append(("cache", asyncio.get_running_loop()))

    def run(**kwargs):
        async def serve():
            runs.append(asyncio.get_running_loop())
            async with server._lifespan(server.mcp):
                pass
        asyncio.run(serve())

    monkeypatch.setattr(server, "prewarm", noop)
    monkeypatch.setattr(server, "_report_agent_problem", noop)
    monkeypatch.setattr(oauth, "finish_refreshes", refreshes)
    monkeypatch.setattr(server.pool, "shutdown", shutdown)
    monkeypatch.setattr(server.cache, "close", close)
    monkeypatch.setattr(server.downloads, "purge_expired", lambda: 0)
    monkeypatch.setattr(server.mcp, "run", run)
    server.run()
    assert closed == [(name, runs[0]) for name in ("refreshes", "browser", "cache")]

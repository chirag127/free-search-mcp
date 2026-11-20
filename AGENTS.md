# Free Search MCP — workspace fork

Read [workspace policy](../../../AGENTS.md), [fork policy](../AGENTS.md), and the
[contribution program](../../../docs/okf/concepts/free-search-mcp-contribution-program.md).
Workspace research lives once in `C:/ws/docs/okf/`; upstream documentation remains
in its native locations and must not be copied into that collection.

## Contribution boundary

- `origin` is the personal fork; `upstream` is `sweetcornna/free-search-mcp`.
- Keep these workspace adapters on `chore/workspace-onboarding`. Create upstream
  contribution branches from a freshly fetched `upstream/main`, not this branch.
- Do not include workspace policy, personal branding, or research ledgers in
  upstream pull requests. Preserve upstream package names and release ownership.
- The user approved a small first batch of reproduced correctness fixes and
  upstream submission, starting with a maintainer-coordination issue about scope
  and templates. Further feature batches require their own approval.
- Use at most two independent workers. Serialize edits to shared schemas,
  registries, configuration, dependency metadata, and lockfiles.
- Research public repositories remotely first. Do not clone reference projects
  here or open private employer documents as contribution sources.

## Baseline and development

- Python >=3.11; `.python-version` selects 3.12. Use the committed uv lockfile.
- Install: `uv sync --locked --group dev`.
- Lint: `uv run ruff check .`.
- Offline tests: `uv run python -m pytest -q`.
- `tests/conftest.py` isolates configuration and mocks network/browser paths.
  Keep `SEARCH_MCP_TEST_NETWORK` unset for the offline suite.
- Live smoke: `uv run python scripts/smoke_mcp.py`; read the script first, use
  disposable configuration/cache/document directories, disable answer backends
  and account-backed engine discovery, and bound external requests.
- Never point tests at the user's live search profile, credentials, or cache.
  Do not enable paid providers, extract credentials, bypass network controls, or
  launch browser sign-ins to make a test pass.
- Follow existing async engines, shared sessions, result models, cache, and
  explicit error contracts; pair behavioral changes with focused regression tests.
- Distinguish a failed launcher, failed test, skipped live probe, and successful
  protocol handshake in reports. Do not describe baseline failures as new defects
  without a reproduction and comparison.

## Documentation and review

- Read `README.md`, `docs/HOW_IT_WORKS.md`, and the relevant subsystem guide before
  proposing changes. Check existing issues and merged work for overlap.
- New contribution text is English; preserve existing translations unless that
  contribution explicitly changes them.
- Each candidate needs evidence, acceptance tests, owned paths, dependencies,
  and a bounded scope. A candidate is not an implemented feature or submitted PR.
- Respect upstream templates when available. Request missing templates before
  launching a large contribution series; do not flood the review queue.
- No releases, force-pushes, or live harness reconfiguration are authorized by
  onboarding. Never stage unrelated umbrella or submodule changes.

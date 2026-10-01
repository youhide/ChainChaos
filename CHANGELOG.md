# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- The crates are marked `publish = false`; ChainChaos is distributed through
  Homebrew, Docker images and GitHub releases, not crates.io.

## [0.2.0] - 2026-10-01

### Added

- JSON-RPC requests sent over a WebSocket now go through the same fault
  pipeline as HTTP: delays, errors, timeouts, response faults, lag views and
  the reorg view. Faulted requests run concurrently and their responses are
  matched by id, so a delayed request does not block others.
- Prometheus metrics at `GET /metrics`: requests, faults by type and rule,
  reorgs, upstream errors, WebSocket connections and notifications.
- Docker images at `ghcr.io/youhide/chainchaos` (linux/amd64, linux/arm64)
  and a source `Dockerfile`.
- `CHAINCHAOS_LISTEN` environment variable for `--listen` (the Docker images
  default to `0.0.0.0:9545`).
- `examples/indexer`: a reorg-aware indexer with an end-to-end test that runs
  it through chainchaos against Anvil.
- Dependabot for Cargo and GitHub Actions.

### Changed

- The HTTP fault pipeline moved into a transport-neutral module shared with
  WebSocket.
- `after_requests` / `for_requests` windows count JSON-RPC requests from both
  transports.

## [0.1.0] - 2026-10-01

First release. Implements the whole initial roadmap:

- **Phase 0:** transparent HTTP JSON-RPC proxy with byte-exact forwarding,
  structured tracing and graceful shutdown.
- **Phase 1:** `delay`, `timeout`, `http_error`, `receipt_null`,
  `transaction_submit_timeout`.
- **Phase 2:** YAML scenarios with time and request-count windows, seeded
  probability and logged transitions.
- **Phase 3:** `stale_head`, `inconsistent_head`, `missing_logs`,
  `duplicated_logs`, `reordered_logs`, `stale_nonce`, `receipt_disappear`,
  `block_null`.
- **Phase 4:** synthetic reorgs with coherent hashes, parent links, logs and
  receipts (`reinclude` or `drop`).
- **Phase 5:** WebSocket proxying with `ws_disconnect`, `ws_delay`,
  `ws_duplicate`, `ws_drop`, `ws_reorder`, `ws_stale`.
- **Phases 6-7:** `record` / `replay` with the versioned `.ccr` format, and
  faults on top of replays.
- Homebrew tap, prebuilt macOS and Linux binaries, Anvil end-to-end tests.

[Unreleased]: https://github.com/youhide/ChainChaos/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/youhide/ChainChaos/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/youhide/ChainChaos/releases/tag/v0.1.0

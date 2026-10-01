# ChainChaos

**A blockchain-aware JSON-RPC chaos testing proxy for EVM applications.**

[![CI](https://github.com/youhide/ChainChaos/actions/workflows/ci.yml/badge.svg)](https://github.com/youhide/ChainChaos/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

```
your application ──▶ chainchaos ──▶ Ethereum-compatible JSON-RPC
                        │            (Anvil, Reth, Geth, a hosted provider…)
                        └── injects realistic RPC and chain-level failures,
                            deterministically
```

---

## The problem

Blockchain applications talk to the chain through JSON-RPC, and JSON-RPC in
production is messy. It fails in ways local development almost never shows:

- **The provider is slow or rate-limited.** `eth_getLogs` takes 8 seconds, then
  you get HTTP 429 for the next minute.
- **The node is behind.** `eth_blockNumber` says 1000, but
  `eth_getBlockByNumber(1000)` returns `null` on the next call because it hit a
  different backend.
- **Receipts aren't there yet.** The transaction is mined, but
  `eth_getTransactionReceipt` returns `null` for a few more seconds, or shows up
  once and then disappears.
- **Submission is ambiguous.** `eth_sendRawTransaction` times out, yet the node
  received the transaction. Resubmit blindly and you might double-spend a nonce
  or pay twice.
- **Logs are inconsistent.** A range query comes back missing events,
  duplicated events, or events in an unexpected order.
- **The chain reorganises.** Blocks you already indexed stop being canonical,
  and their receipts and logs disappear or move.
- **Subscriptions misbehave.** `newHeads` skips a block, delivers one twice, goes
  silent while the socket stays open, or the connection just drops.

Most wallets, indexers, bots, relayers and backends are tested against a local
node that is fast, consistent and never reorgs. The code paths that handle the
failures above get exercised for the first time in production.

### Why a generic fault proxy is not enough

Tools like Toxiproxy are excellent at transport faults: latency, dropped
connections, bandwidth limits. But the hard bugs in blockchain applications
live *above* the transport layer. A TCP proxy cannot make a receipt temporarily
`null`, report a stale head, or make a block hash change under an indexer,
because it does not know what a receipt, a head or a block hash is.

ChainChaos does. Conceptually it is:

> **Toxiproxy + record/replay + blockchain semantics.**

It sits between your application and a real EVM JSON-RPC endpoint (or a
recording of one), forwards traffic transparently, and injects realistic RPC
and chain-level failures in a **deterministic, reproducible** way, without
changing your application.

### What it is not

ChainChaos is **not** a blockchain emulator. It does not execute transactions
or run consensus. It relies on a real upstream node (or a recording) and only
reshapes what your application *observes*: timing, transport behaviour and
response contents. It never creates, modifies or re-signs transactions.

## Quick start

Install with Homebrew (macOS and Linux, prebuilt binaries):

```bash
brew install youhide/youhide/chainchaos
```

Or build from source with a stable Rust toolchain (1.85 or newer):

```bash
cargo install --git https://github.com/youhide/ChainChaos chainchaos
```

Start a local node (for example `anvil`, which serves HTTP and WebSocket on port
8545), then:

```bash
chainchaos proxy --upstream http://127.0.0.1:8545
```

Point your application at `http://127.0.0.1:9545` (and `ws://127.0.0.1:9545`
for subscriptions). Without a scenario ChainChaos is fully transparent: the
upstream's status code, headers and body bytes are relayed unchanged.

Now add some chaos:

```bash
chainchaos proxy --upstream http://127.0.0.1:8545 --scenario scenarios/unreliable-rpc.yaml
```

## Use cases

### Testing an indexer against a reorg

```yaml
# scenarios/reorg.yaml (excerpt)
scenario:
  - after_requests: 50
    inject: { type: reorg, depth: 3 }
```

After the 50th request, blocks `head-2 ..= head` get new hashes, linked into a
coherent new branch. Queries by the old hashes return `null` (or
`unknown block` for `eth_getLogs`), and logs and receipts point at the new
branch. The indexer should detect the non-canonical blocks, roll back and
reprocess them. With `transactions: drop`, the transactions vanish from the new
branch instead: receipts turn `null`, logs disappear, and the transactions look
pending again.

### Testing ambiguous transaction submission

```yaml
faults:
  - { type: transaction_submit_timeout, duration: 60s, count: 1 }
```

`eth_sendRawTransaction` reaches the node, which accepts it, but your
application gets a timeout. Does it check the transaction hash or nonce before
resubmitting, or does it blindly send again?

### Testing RPC reliability over time

```yaml
seed: 42
scenario:
  - after: 10s
    for: 20s
    inject: { type: delay, duration: 1500ms }
  - after: 20s
    for: 10s
    inject: { type: http_error, status: 429, probability: 0.5 }
  - after: 40s
    for: 5s
    inject: { type: receipt_null }
  - after: 60s
    inject: { type: reorg, depth: 2 }
```

### Turning a staging bug into a CI fixture

```bash
chainchaos record --upstream https://staging-rpc.example --output bug-1234.ccr
# ...reproduce the bug through http://127.0.0.1:9545...
chainchaos replay bug-1234.ccr --scenario scenarios/reorg.yaml   # in CI, no RPC needed
```

## CLI

```text
chainchaos proxy  --upstream <URL> [--upstream-ws <URL>] [--scenario <FILE>] [--seed <N>]
                  [--listen <ADDR>] [--upstream-timeout <DURATION>]
chainchaos record --upstream <URL> --output <FILE> [--redact-field <KEY>]...
                  [--listen <ADDR>] [--upstream-timeout <DURATION>]
chainchaos replay <FILE> [--scenario <FILE>] [--seed <N>] [--replay-latency] [--listen <ADDR>]
```

| Option               | Default          | Meaning                                                            |
| -------------------- | ---------------- | ------------------------------------------------------------------ |
| `--upstream`         |                  | Upstream EVM JSON-RPC endpoint (http or https).                    |
| `--upstream-ws`      | derived          | WebSocket upstream. Defaults to `--upstream` with ws/wss (as on Anvil); pass it explicitly for Geth's separate port. |
| `--listen`           | `127.0.0.1:9545` | Address for both HTTP (`POST /`) and WebSocket (`GET /`).          |
| `--scenario`         |                  | YAML fault rules and/or timed scenario (alias: `--config`).        |
| `--seed`             | scenario's seed  | Override the scenario seed.                                        |
| `--upstream-timeout` | `30s`            | Answer 504 if the upstream takes longer.                           |
| `--output`, `-o`     |                  | `record`: file to write.                                           |
| `--redact-field`     |                  | `record`: replace this JSON key's value everywhere. Repeatable.    |
| `--replay-latency`   | off              | `replay`: delay responses by their recorded upstream latency.      |

Durations are written as `250ms`, `2s` or `1m`. Logging is controlled with
`RUST_LOG` (default `info`).

## Scenario files

One YAML format covers static rules and timed scenarios. Bundled examples live
in [`scenarios/`](scenarios) and [`examples/`](examples).

```yaml
seed: 42              # drives every random decision (default 0)

faults:               # rules, active from the start unless windowed
  - type: delay
    method: eth_getLogs
    duration: 1500ms
    probability: 0.5

scenario:             # ordered, timed steps
  - after: 20s        # window fields live on the step...
    for: 10s
    inject:           # ...and the fault in `inject`
      type: http_error
      status: 429
```

### Common rule fields

| Field            | Meaning                                                                 |
| ---------------- | ----------------------------------------------------------------------- |
| `type`           | The fault (see below). Required.                                        |
| `method`         | Only requests calling this JSON-RPC method. Must be within the fault's scope. |
| `subscription`   | `ws_*` faults only: only this subscription type (`newHeads`, `logs`, ...). |
| `count`          | Fire at most this many times. `reorg` defaults to 1.                    |
| `probability`    | Chance of firing per matching request, from the seeded RNG. `(0, 1]`.   |
| `after`          | Window opens this long after start.                                     |
| `after_requests` | Window opens after this many requests (the next one is the first affected). |
| `for`            | Window stays open this long.                                            |
| `for_requests`   | Window stays open for this many requests.                               |

Semantics:

- **Every matching rule fires, in order** (`faults` first, then `scenario`).
  Two matching delays add up; faults compose.
- **Determinism:** probability rolls and per-fault randomness (which logs to
  drop, how to shuffle, synthetic reorg hashes) are derived from
  `(seed, rule, request number)`, so the same scenario, seed and request
  sequence reproduce the same behaviour. Request-count windows are exact;
  time windows depend on wall-clock timing by nature.
- **HTTP and WebSocket are counted separately:** `after_requests` counts HTTP
  requests for HTTP faults and subscription notifications for `ws_*` faults.
- **Batches:** transport faults (delay, errors, timeouts) affect the whole
  batch if any call matches; response faults only touch matching calls.
- **Strict parsing:** unknown fields, out-of-range values and methods outside a
  fault's scope are rejected at startup, so a typo never silently disables a
  fault.

### Faults

**Transport and HTTP (Phase 1)**

| Type                         | Fields                                     | Behaviour |
| ---------------------------- | ------------------------------------------ | --------- |
| `delay`                      | `duration`                                 | Hold the request before forwarding it. |
| `timeout`                    | `duration` (60s), `forward` (false)        | Hold the connection, then answer 504. With `forward: true` the request reaches the upstream first. |
| `http_error`                 | `status` (400-599), `body`, `retry_after`  | Answer with this status instead of forwarding. Default body is a JSON-RPC error (`-32005` for 429). |
| `receipt_null`               |                                            | `eth_getTransactionReceipt` returns `null` even when the receipt exists. |
| `transaction_submit_timeout` | `duration` (60s)                           | Forward `eth_sendRawTransaction`, discard the answer, time out the client. |

**Blockchain-aware (Phase 3)**

| Type                | Fields                | Behaviour |
| ------------------- | --------------------- | --------- |
| `stale_head`        | `lag`                 | A coherent view `lag` blocks behind: `eth_blockNumber`, `latest` tags, blocks, logs, receipts and transactions above the stale head are hidden. |
| `inconsistent_head` | `lag`                 | `eth_blockNumber` reports the real head, but block/log/receipt queries see a node `lag` blocks behind (load-balanced backends disagreeing). |
| `missing_logs`      | `ratio` (0.5)         | Drop roughly `ratio` of the logs (at least one). |
| `duplicated_logs`   | `ratio` (0.5)         | Duplicate roughly `ratio` of the logs, next to the originals. |
| `reordered_logs`    |                       | Shuffle log order (guaranteed different). |
| `stale_nonce`       | `lag` (1)             | `eth_getTransactionCount` reports `lag` less. |
| `receipt_disappear` | `visible_for` (1)     | A receipt is served `visible_for` times, then returns `null`. |
| `block_null`        |                       | Block queries return `null`. |

**Chain reorganisation (Phase 4)**

| Type    | Fields                                             | Behaviour |
| ------- | -------------------------------------------------- | --------- |
| `reorg` | `depth` (1-128), `head` (current), `transactions` (`reinclude` \| `drop`) | Replace the last `depth` blocks with a synthetic branch; see [Reorg simulation](#reorg-simulation). |

**WebSocket subscriptions (Phase 5)**

| Type            | Fields              | Behaviour |
| --------------- | ------------------- | --------- |
| `ws_disconnect` | `graceful` (false)  | Drop the connection; with `graceful`, send close code 1012 (service restart) first. |
| `ws_delay`      | `duration`          | Delay a notification (later ones queue behind it). |
| `ws_duplicate`  |                     | Deliver a notification twice. |
| `ws_drop`       |                     | Drop a notification. |
| `ws_reorder`    |                     | Deliver a notification after the next one. |
| `ws_stale`      |                     | Silence notifications while active; the connection stays open. |

## Reorg simulation

ChainChaos does not fork the upstream chain. After a reorg of depth `d` at head
`H`, it rewrites what the client observes about blocks `H-d+1 ..= H`:

```text
before:  N-1 ── N (0xAAA) ── N+1 (0xBBB)            real hashes
after:   N-1 ── N (0xCCC) ── N+1 (0xDDD) ── N+2     synthetic branch
```

- Block hashes in the range are replaced by synthetic hashes derived from
  `(seed, real hash, reorg generation)`; `parentHash` links are rewritten so
  the branch is coherent, including blocks mined after the reorg.
- Logs, receipts and transactions in the range carry the new `blockHash`
  (`reinclude`), or disappear and turn pending (`drop`).
- Queries by an old hash (pre-reorg, or from an earlier synthetic branch)
  return `null`, and `eth_getLogs` by old `blockHash` returns `unknown block`.
- Queries by a synthetic hash are translated back to the real hash before
  forwarding.
- `newHeads` and `logs` subscriptions follow the same view.
- Repeated reorgs of the same height produce a fresh branch each time.

The head comes from `head:` in the rule, or from an `eth_blockNumber` call the
proxy makes on its own behalf (the only kind of request chainchaos ever
originates, and always read-only).

## Record and replay

`chainchaos record` is a transparent proxy that also writes every exchange to a
`.ccr` file: JSON Lines with a versioned header.

```text
{"format":"chainchaos-recording","version":1,"recorded_at_unix_ms":…,"upstream":"https://…/","redacted_fields":["apiKey"]}
{"seq":1,"offset_ms":0,"latency_ms":3,"request":{"json":{"jsonrpc":"2.0","method":"eth_chainId"}},"status":200,"response":{"json":{"jsonrpc":"2.0","result":"0x1"}}}
```

- **Request ids are normalised away** and restored from the live request at
  replay time; batch responses are stored in request order.
- **Matching is by content** (canonical JSON, keys sorted, ids removed).
  Repeated identical requests replay their recorded responses in order, and the
  last one repeats. This keeps replay correct under concurrency.
- **Timing is informational:** `recorded_at_unix_ms`, `offset_ms` and
  `latency_ms` never affect matching. `--replay-latency` optionally
  reproduces upstream latency.
- **Redaction** (`--redact-field`) happens before writing and before matching,
  so redacted requests still replay.
- Requests missing from a recording get a JSON-RPC error `-32001` with
  `x-chainchaos-error: replay-miss`.

`chainchaos replay session.ccr --scenario scenarios/reorg.yaml` combines the
two (Phase 7): every fault, reorgs included, works on top of recorded data.

## Observability

Every injected fault is logged with the request's sequence number, method and
JSON-RPC id:

```text
INFO rpc{seq=1 method=eth_blockNumber id=1}: injecting fault fault="stale_head" rule=faults[0]
INFO rpc{seq=2 method=eth_getBlockByNumber id=2}: injecting fault: block tags rewritten to the lagging head fault="stale_head" rule=faults[0] visible_head=26093678
INFO rpc{seq=51 method=eth_getLogs id=51}: injecting fault fault="reorg" rule=scenario[0] first_block=98 last_block=100 generation=1 transactions=Reinclude
INFO scenario step activated rule=scenario[1] fault="http_error" elapsed_ms=20000
INFO ws{conn=1}: injecting fault fault="ws_drop" rule=faults[0] subscription=logs seq=12
```

- `rule=` points at the config entry (`faults[N]` / `scenario[N]`).
- Scenario windows log when they open and close, even without traffic.
- Requests without faults are logged at `debug` (`RUST_LOG=debug` to see all).
- Responses affected by a fault carry `x-chainchaos-faults: delay,receipt_null`.
- Responses generated by chainchaos itself carry `x-chainchaos-error`
  (`upstream`, `injected` or `replay-miss`).
- Credentials and API-key paths in upstream URLs are redacted from logs.

## Transparency guarantees

With no matching faults, ChainChaos:

- forwards the request body byte for byte, including batches, notifications
  and malformed payloads;
- relays the upstream HTTP status, end-to-end headers and body bytes unchanged,
  including JSON-RPC ids and error objects;
- forwards WebSocket messages unchanged;
- never creates, modifies or re-signs transactions.

When a fault must rewrite a response, only the affected fields change; every
other field, including chain-specific extensions, passes through. When the
upstream is unreachable or times out, ChainChaos answers 502/504 with a
well-formed JSON-RPC error (code `-32000`) carrying the request's id.

Current limitations:

- HTTP faults apply to HTTP requests; over WebSocket, only subscription
  notifications are faulted (request/response traffic on the socket passes
  through).
- `record` and `replay` cover HTTP traffic only.
- One upstream at a time.

## Architecture

```
crates/
├── chainchaos-core    Fault model, YAML config, scenario engine, seeded RNG,
│                      JSON-RPC inspection, .ccr recording format. No networking.
├── chainchaos-evm     EVM-aware mutations on serde_json::Value: lag views,
│                      log/nonce faults, the synthetic reorg view. No I/O.
├── chainchaos-proxy   Axum server: HTTP pipeline, WebSocket proxy, upstream
│                      (live or replay), recorder.
└── chainchaos-cli     The `chainchaos` binary: proxy / record / replay.
```

HTTP request flow:

```
plan faults ─▶ pre-forward ─▶ rewrite request ─▶ forward ─▶ post-forward ─▶ respond
               delay           lag views:          live or    tx-submit timeout,
               http_error      `latest` tags       replay     reorg view,
               timeout         reorg: synthetic               response faults
               reorg (event)   → real hashes
```

Design decisions:

- **Raw bytes by default.** Parsing is read-only; the original bytes are
  forwarded and returned unless a fault changes them. Transparency is tested
  byte for byte.
- **`serde_json::Value`, not typed RPC structs.** Round-tripping responses
  through typed structs (Alloy or otherwise) would reformat them and drop
  fields the types do not know, such as L2 receipt extensions. Mutations touch
  only the fields they need. Alloy was considered and left out for this
  reason.
- **Faults are enum variants, not plugins.** Each fault is one `Fault` variant
  plus one arm in the proxy. That is all the composability needed.
- **The engine never reads the clock.** It receives `(request number, elapsed
  time)` and derives randomness from `(seed, rule, request number)`, so it is
  deterministic and unit-testable without sleeping.
- **In-house PRNG (SplitMix64).** Its output must never change between
  releases, or recorded scenarios would stop reproducing; a test pins it.
- **Reorgs rewrite views, not chains.** Synthetic hashes are computed lazily
  from real hashes, so reorgs work on live nodes and on recordings alike,
  without prefetching.

## Development

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

Integration tests (`crates/chainchaos-proxy/tests/`) run against an in-process
fake EVM node that serves HTTP and WebSocket on one port with a deterministic
chain, so no real node is needed. `tests/anvil.rs` additionally runs end-to-end
scenarios against a real Anvil node when `anvil` is on PATH (set
`CHAINCHAOS_REQUIRE_ANVIL=1` to make a missing Anvil an error). CI runs the
commands above on Linux and macOS, the Anvil suite with Foundry installed, and
a build on the minimum supported Rust version (1.85).

Releases: pushing a `vX.Y.Z` tag that matches the workspace version builds
macOS and Linux binaries, publishes a GitHub release and updates the Homebrew
formula in [youhide/homebrew-youhide](https://github.com/youhide/homebrew-youhide).

## Roadmap

ChainChaos was built in small, testable phases, each with a concrete success
criterion. All seven are implemented.

### Phase 0: Foundation ✅

Cargo workspace · CLI · transparent HTTP JSON-RPC proxy · configurable listen
address and upstream · byte-exact forwarding · structured tracing · graceful
shutdown · clear errors · unit and integration tests.

**Success criterion:** `chainchaos proxy --upstream http://127.0.0.1:8545` works
as a transparent EVM JSON-RPC proxy without changing responses.

### Phase 1: Basic fault injection ✅

- [x] Rule framework: method matching, fault scopes, `count`, per-fault logging
- [x] `delay`, `timeout`, `http_error` (429/500/502/503/…)
- [x] `receipt_null`
- [x] `transaction_submit_timeout` (ambiguous submission)

**Success criterion:** users can reliably reproduce common RPC failure
behaviour without modifying their application.

### Phase 2: Scenario engine ✅

- [x] YAML scenarios with ordered steps
- [x] Time triggers and windows (`after`, `for`)
- [x] Request-count triggers and windows (`after_requests`, `for_requests`)
- [x] Method filters, seeded `probability`, explicit `seed` / `--seed`
- [x] Event logging of window transitions

**Success criterion:** the same scenario and seed reproduce the same behaviour.

### Phase 3: Blockchain-aware faults ✅

- [x] Stale head (coherent lagging view)
- [x] Missing, duplicated and reordered logs
- [x] Stale nonce
- [x] Inconsistent latest block across `eth_blockNumber`,
      `eth_getBlockByNumber`, `eth_getLogs` and `eth_getTransactionReceipt`
- [x] Disappearing receipts
- [x] Block response mutation (`block_null`, lag views)
- [x] Valid JSON-RPC and EVM data shapes preserved

**Success criterion:** developers can test against plausible blockchain
inconsistencies, not only network failures.

### Phase 4: Reorg simulation ✅

- [x] Configurable depth and head (affected range)
- [x] Changed block hashes and coherent parent links
- [x] Logs removed from the old branch, replacement logs on the new branch
- [x] Receipts becoming non-canonical (`transactions: drop`)
- [x] Old hashes non-canonical, synthetic hashes queryable, repeated reorgs

**Success criterion:** an indexer can be integration-tested against
deterministic reorg scenarios.

### Phase 5: WebSocket support ✅

- [x] WebSocket proxying with `eth_subscribe` (`newHeads`, `logs`, …)
- [x] Dropped connection / forced reconnect, delayed, duplicated, missing and
      reordered messages, stale subscription stream
- [x] Subscriptions follow the reorged view

**Success criterion:** real-time blockchain consumers can be chaos-tested.

### Phase 6: Record and replay ✅

- [x] `chainchaos record` → versioned `.ccr` format
- [x] `chainchaos replay` without the original upstream
- [x] Request id normalisation, ordering, concurrency-safe matching
- [x] Configurable redaction
- [x] Wall-clock timestamps separate from deterministic replay

**Success criterion:** an RPC bug observed in development or staging becomes a
reproducible CI fixture.

### Phase 7: Replay + chaos ✅

- [x] `chainchaos replay session.ccr --scenario scenarios/reorg.yaml`

**Success criterion:** production-like scenarios become repeatable
integration tests.

### Future ideas

Not planned yet: multiple upstream providers and upstream disagreement
simulation · execution client differential testing · Foundry / Anvil helpers ·
CI integrations · Docker image · Prometheus metrics · a larger scenario
library · fuzz-generated scenarios · stateful mempool and transaction lifecycle
simulation · faults for WebSocket request/response traffic · WebSocket
record/replay · other blockchain ecosystems.

## Principles

1. **Rust-native.** Tokio, a single binary, low overhead, no Node.js or JVM.
2. **EVM-first.** Ethereum-compatible JSON-RPC only; no premature multi-chain
   abstraction.
3. **Proxy-first.** Transparent by default.
4. **Deterministic.** Scenarios are reproducible; randomness always takes a seed.
5. **Safe by default.** Never modifies or generates transactions; mutates
   responses and transport behaviour only.
6. **Composable.** Faults are independent and can be combined.
7. **Observable.** Every injected fault is logged and traceable.
8. **Not overengineered.** No plugins, databases or distributed architecture.

## Contributing

Issues and pull requests are welcome. Please run the three
[development](#development) commands before opening a PR. New faults should
come with an integration test showing both the injected behaviour and that
non-matching traffic is left untouched.

## License

[MIT](LICENSE)

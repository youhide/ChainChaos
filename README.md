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

> **Status: early.** Phase 0 (transparent proxy) and the Phase 1 fault
> framework are in place, with one fault type (`delay`) implemented. See the
> [roadmap](#roadmap).

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
  `eth_getTransactionReceipt` returns `null` for a few more seconds.
- **Submission is ambiguous.** `eth_sendRawTransaction` times out, yet the node
  received the transaction. Resubmit blindly and you might double-spend a nonce
  or pay twice.
- **Logs are inconsistent.** A range query comes back missing events,
  duplicated events, or events in an unexpected order.
- **The chain reorganises.** Blocks you already indexed stop being canonical,
  and their receipts and logs disappear.

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

It sits between your application and a real EVM JSON-RPC endpoint, forwards
traffic transparently, and injects realistic RPC and chain-level failures in a
**deterministic, reproducible** way, without changing your application.

### What it is not

ChainChaos is **not** a blockchain emulator. It does not execute transactions
or run consensus. It relies on a real upstream node and only reshapes what your
application *observes*: timing, transport behaviour and, in later phases,
response contents.

## Example use cases

**Testing an indexer.** Put ChainChaos between your indexer and Anvil, inject a
reorg of depth 2, and assert that the indexer detects the non-canonical blocks,
rolls back and reprocesses them. *(Phase 4)*

**Testing transaction submission.** ChainChaos forwards `eth_sendRawTransaction`
to the node, which accepts it, but returns a timeout to your application. Does
it check the nonce or tx hash before resubmitting, or does it blindly send
again? *(Phase 1)*

**Testing RPC reliability.** Describe an adverse timeline (slow responses
after 10s, a burst of 429s after 20s, null receipts after 40s) and replay it
with the same seed in CI. *(Phase 2)*

**Today:** add latency to specific RPC methods to check that timeouts, retries
and loading states behave correctly.

## Quick start

Requires a stable Rust toolchain (1.85 or newer).

```bash
git clone https://github.com/youhide/ChainChaos.git
cd ChainChaos
cargo install --path crates/chainchaos-cli
```

Start a local node (for example `anvil`, which listens on port 8545), then:

```bash
chainchaos proxy --upstream http://127.0.0.1:8545 --listen 127.0.0.1:9545
```

Point your application at `http://127.0.0.1:9545` instead of the node. Without
a config file ChainChaos is fully transparent: the upstream's status code,
headers and body bytes are relayed unchanged.

Now add some chaos:

```bash
chainchaos proxy --upstream http://127.0.0.1:8545 --config examples/delay.yaml
```

### CLI reference

```text
chainchaos proxy [OPTIONS] --upstream <URL>

  --upstream <URL>               Upstream EVM JSON-RPC endpoint (http or https)
  --listen <ADDR>                Address to listen on [default: 127.0.0.1:9545]
  --config <FILE>                YAML file with fault rules
  --upstream-timeout <DURATION>  Upstream timeout before answering 504 [default: 30s]
```

Durations are written as `250ms`, `2s` or `1m`.

## Fault configuration

Faults are declared as rules in a YAML file:

```yaml
faults:
  # Slow down every log query by 1.5s.
  - type: delay
    method: eth_getLogs
    duration: 1500ms

  # Delay only the first three receipt lookups.
  - type: delay
    method: eth_getTransactionReceipt
    duration: 2s
    count: 3
```

| Field    | Required | Meaning                                                                 |
| -------- | -------- | ----------------------------------------------------------------------- |
| `type`   | yes      | Fault type. Currently only `delay`.                                     |
| `method` | no       | Only match requests calling this JSON-RPC method. Omit to match all.    |
| `count`  | no       | Fire at most this many times. Omit for unlimited.                       |
| `duration` | for `delay` | How long to hold the request before forwarding it upstream.            |

Matching semantics:

- **Every matching rule fires, in file order.** Two matching delays add up.
- **Batches:** a rule with `method` matches a batch if *any* call in it uses
  that method, and the fault applies to the whole batch.
- **Non-JSON-RPC bodies** are still forwarded; only rules without `method`
  apply to them.
- **Unknown fields are rejected**, so a typo such as `duraton` fails at startup
  instead of silently disabling a fault.

### Implemented faults

| Fault   | Status | Behaviour                                                 |
| ------- | ------ | --------------------------------------------------------- |
| `delay` | ✅     | Holds the request for `duration`, then forwards it normally. |

The remaining Phase 1 faults (`timeout`, `http_error`, `receipt_null`,
`transaction_submit_timeout`) are next; see the [roadmap](#roadmap).

## Observability

Every injected fault is logged with the request's sequence number, method and
JSON-RPC id, so you can tell exactly why a client got the response it did:

```text
INFO rpc{seq=1 method=eth_chainId id="abc"}: injecting fault fault="delay" rule=2 delay_ms=50
INFO rpc{seq=1 method=eth_chainId id="abc"}: request completed status=200 elapsed_ms=323 faults=1
```

- `rule=N` points to `faults[N]` in your config.
- Requests that received no fault are logged at `debug` level. Use
  `RUST_LOG=debug` to see every request, or `RUST_LOG=warn` to keep it quiet.
- Responses affected by a fault carry an `x-chainchaos-faults` header, for
  example `x-chainchaos-faults: delay`.
- Credentials and API-key paths in the upstream URL are redacted from logs.

## Transparency guarantees

With no matching faults, ChainChaos:

- forwards the request body byte for byte, including batches, notifications
  and malformed payloads;
- relays the upstream HTTP status, end-to-end headers and body bytes unchanged,
  including JSON-RPC ids and JSON-RPC error objects;
- never creates, modifies or re-signs transactions.

The one case where ChainChaos writes its own response is when the upstream
cannot be reached or times out. It then returns HTTP 502 or 504 with a
well-formed JSON-RPC error (code `-32000`) carrying the request's id, plus an
`x-chainchaos-error: upstream` header, so these are never confused with real
upstream errors.

Current limitations: HTTP only (no WebSocket yet), a single upstream, and
requests are accepted on `POST /` only.

## Architecture

```
crates/
├── chainchaos-core    Fault model, YAML config, JSON-RPC request inspection,
│                      and the engine that decides which faults apply.
│                      No networking.
├── chainchaos-proxy   Axum server + reqwest client. Forwards requests and
│                      realises the faults the engine selects.
└── chainchaos-cli     The `chainchaos` binary: argument parsing, logging,
                       graceful shutdown.
```

Request flow:

```
client ──POST──▶ proxy: parse JSON-RPC (read-only; raw bytes are kept)
                   │
                   ├─▶ engine.plan(request) ─▶ [InjectedFault, …]
                   │
                   ├─▶ pre-forward faults     (delay today; timeout/http_error next)
                   ├─▶ forward raw bytes upstream
                   ├─▶ post-forward faults    (receipt_null, tx-submit-timeout: Phase 1)
                   │
client ◀─────────── response (+ x-chainchaos-faults when a fault fired)
```

Design decisions:

- **Raw bytes are forwarded, not re-serialised.** Parsing is read-only, so the
  proxy cannot accidentally reorder keys, change number formatting or drop
  fields. Transparency is tested byte for byte.
- **Faults are plain enum variants, not plugins.** Each fault is one variant
  of `Fault` plus one match arm in the proxy. That is enough composability for
  now; a plugin system would be premature.
- **The engine is transport-agnostic.** `chainchaos-core` decides *what* to
  inject and the transport decides *how*, so the same rules can drive
  WebSocket (Phase 5) and replay (Phase 6) later.
- **No EVM crate yet.** Blockchain-aware logic (receipts, logs, blocks, reorgs)
  gets its own crate in Phase 3, once there is real code to put in it. Alloy
  will be adopted there if its types remove real duplication.
- **Responses are buffered.** That costs a little memory for large
  `eth_getLogs` responses, but response-mutating faults need the full body.

## Development

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

The integration tests in `crates/chainchaos-proxy/tests/` spin up an
in-process fake upstream, so no node is needed. CI runs the same commands on
Linux and macOS, plus a build on the minimum supported Rust version (1.85).

## Roadmap

ChainChaos is built in small, testable steps. Each phase has a concrete
success criterion.

### Phase 0: Foundation ✅

Goal: a clean Rust project and a transparent JSON-RPC proxy.

- [x] Cargo workspace
- [x] CLI (`chainchaos proxy`)
- [x] HTTP JSON-RPC proxy with configurable listen address and upstream URL
- [x] Byte-exact request and response forwarding
- [x] Structured tracing
- [x] Graceful shutdown (Ctrl-C / SIGTERM, drains in-flight requests)
- [x] Clear error messages (bad config, bad upstream, unreachable upstream)
- [x] Unit and integration test foundation

**Success criterion:** `chainchaos proxy --upstream http://127.0.0.1:8545`
works as a transparent EVM JSON-RPC proxy without changing responses.

### Phase 1: Basic fault injection 🚧

Goal: make ChainChaos useful immediately.

- [x] Fault rule framework: method matching, `count` limits, per-fault logging
- [x] `delay`: hold selected requests before forwarding
- [ ] `timeout`: make the caller experience a timeout, optionally without forwarding
- [ ] `http_error`: return configurable 429 / 500 / 502 / 503 responses
- [ ] `receipt_null`: return `null` for `eth_getTransactionReceipt` even when the upstream has a receipt
- [ ] `transaction_submit_timeout`: forward `eth_sendRawTransaction` upstream, discard the response, and time out the client, reproducing an ambiguous submission

**Success criterion:** users can reliably reproduce common RPC failure behaviour
without modifying their application.

### Phase 2: Scenario engine

Goal: deterministic, repeatable chaos tests.

- [ ] YAML scenario files with ordered, timed actions (`after: 10s`)
- [ ] Request-count based triggers
- [ ] Enable/disable windows (`duration:` on an injected fault)
- [ ] Probability where useful, always driven by an explicit `seed`
- [ ] Clear event logging of scenario transitions
- [ ] `--scenario` CLI flag

```yaml
seed: 42
scenario:
  - after: 10s
    inject: { type: delay, method: eth_getLogs, duration: 2s }
  - after: 20s
    inject: { type: http_error, status: 429, duration: 5s }
```

**Success criterion:** the same scenario and seed reproduce the same behaviour.

### Phase 3: Blockchain-aware faults

Goal: what makes ChainChaos different from generic proxy tools.

- [ ] Stale head
- [ ] Missing, duplicated and reordered logs
- [ ] Stale nonce observations
- [ ] Inconsistent `latest` block
- [ ] Disappearing receipts
- [ ] Block response mutation where safe
- [ ] Temporary inconsistencies between `eth_blockNumber`,
      `eth_getBlockByNumber`, `eth_getLogs` and `eth_getTransactionReceipt`

All mutations must preserve valid JSON-RPC and valid EVM data shapes.

**Success criterion:** developers can test against plausible blockchain
inconsistencies, not only network failures.

### Phase 4: Reorg simulation

Goal: realistic application-level reorg testing.

No real fork is produced. Instead, ChainChaos maintains a synthetic view in
which the client first sees `block N = 0xAAA…, N+1 = 0xBBB…` and later
`block N = 0xCCC…, N+1 = 0xDDD…`, with responses kept internally coherent.

- [ ] Configurable reorg depth and affected block range
- [ ] Changed block hashes (and parent hashes)
- [ ] Logs removed from the old branch, replacement logs on the new branch
- [ ] Receipts becoming non-canonical where applicable

This will be designed carefully, and only after the simpler fault engine is
stable.

**Success criterion:** an indexer can be integration-tested against
deterministic reorg scenarios.

### Phase 5: WebSocket support

- [ ] WebSocket proxying, including `eth_subscribe` for `newHeads` and `logs`
- [ ] Faults: dropped connection, delayed / duplicated / missing / reordered
      messages, forced reconnect, stale subscription stream

**Success criterion:** real-time blockchain consumers can be chaos-tested.

### Phase 6: Record and replay

- [ ] `chainchaos record`: capture JSON-RPC interactions into a versioned,
      deterministic recording format
- [ ] `chainchaos replay session.ccr`: serve recorded responses without the
      original upstream
- [ ] Request id normalisation, ordering preservation, concurrent requests
- [ ] Configurable redaction of sensitive data
- [ ] Wall-clock timestamps recorded separately from deterministic replay timing

**Success criterion:** an RPC bug observed in development or staging becomes a
reproducible CI fixture.

### Phase 7: Replay + chaos

```bash
chainchaos replay session.ccr --scenario scenarios/reorg.yaml
```

Deterministic RPC fixtures combined with deterministic faults, so
production-like scenarios become repeatable integration tests.

### Future ideas (not planned yet)

Multiple upstream providers · upstream disagreement simulation · execution
client differential testing · Foundry / Anvil helpers · CI integrations ·
Docker image · Prometheus metrics · scenario library · fuzz-generated scenarios
· stateful mempool and transaction lifecycle simulation · other blockchain
ecosystems.

## Principles

1. **Rust-native.** Tokio, a single binary, low overhead, no Node.js or JVM.
2. **EVM-first.** Ethereum-compatible JSON-RPC only; no premature multi-chain
   abstraction.
3. **Proxy-first.** Transparent HTTP first, WebSocket next.
4. **Deterministic.** Scenarios are reproducible; randomness always takes a seed.
5. **Safe by default.** Never modifies or generates transactions unless
   explicitly told to; mutates responses and transport behaviour only.
6. **Composable.** Faults are independent and can be combined.
7. **Observable.** Every injected fault is logged and traceable.
8. **Not overengineered.** The smallest useful thing first: no plugins,
   databases or distributed architecture until they are clearly needed.

## Contributing

Issues and pull requests are welcome. Please run the three
[development](#development) commands before opening a PR. New faults should
come with an integration test showing both the injected behaviour and that
non-matching traffic is left untouched.

## License

[MIT](LICENSE)

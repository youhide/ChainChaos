# chainchaos-evm

Part of [ChainChaos](https://github.com/youhide/ChainChaos), a blockchain-aware JSON-RPC chaos testing proxy for EVM applications.

EVM-aware JSON-RPC mutations on `serde_json::Value`: lagging chain views (stale / inconsistent head), log and nonce faults, and the synthetic reorg view. Pure functions, no I/O.

Most users want the `chainchaos` command-line tool instead:

```bash
cargo install chainchaos
```

License: MIT

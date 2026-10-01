# chainchaos-core

Part of [ChainChaos](https://github.com/youhide/ChainChaos), a blockchain-aware JSON-RPC chaos testing proxy for EVM applications.

Transport-agnostic building blocks: the fault model, YAML scenario parsing, the deterministic scenario engine (seeded SplitMix64), JSON-RPC request inspection and the versioned `.ccr` recording format.

Most users want the `chainchaos` command-line tool instead:

```bash
cargo install chainchaos
```

License: MIT

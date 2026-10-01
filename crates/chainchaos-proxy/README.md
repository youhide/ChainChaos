# chainchaos-proxy

Part of [ChainChaos](https://github.com/youhide/ChainChaos), a blockchain-aware JSON-RPC chaos testing proxy for EVM applications.

The axum-based proxy: HTTP and WebSocket JSON-RPC pipeline, live or replayed upstreams, recording, and Prometheus metrics. Embed it with `Proxy::new(config)?.serve(listener, shutdown)`.

Most users want the `chainchaos` command-line tool instead:

```bash
cargo install chainchaos
```

License: MIT

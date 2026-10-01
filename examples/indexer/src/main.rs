//! Runs the example indexer against an RPC endpoint (usually chainchaos):
//!
//! ```bash
//! anvil &
//! chainchaos proxy --upstream http://127.0.0.1:8545 --scenario scenarios/reorg.yaml &
//! cargo run -p chainchaos-example-indexer -- http://127.0.0.1:9545
//! ```

use std::time::Duration;

use chainchaos_example_indexer::Indexer;

#[tokio::main]
async fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:9545".to_owned());
    let mut indexer = Indexer::new(url);
    loop {
        match indexer.sync().await {
            Ok(report) => {
                let tip = indexer.blocks().values().next_back();
                println!(
                    "indexed={} rolled_back={} reorgs={} tip={}",
                    report.indexed,
                    report.rolled_back,
                    indexer.reorgs_detected,
                    tip.map_or_else(|| "-".to_owned(), |b| format!("{} {}", b.number, b.hash)),
                );
            }
            Err(e) => eprintln!("sync failed: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

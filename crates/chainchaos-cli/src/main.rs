use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use chainchaos_core::FaultConfig;
use chainchaos_core::duration::parse_duration;
use chainchaos_core::recording::Recording;
use chainchaos_proxy::{Proxy, ProxyConfig, RecordConfig, UpstreamConfig};
use clap::{Args, Parser, Subcommand};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use url::Url;

/// Blockchain-aware JSON-RPC chaos testing proxy.
#[derive(Debug, Parser)]
#[command(name = "chainchaos", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Proxy a live JSON-RPC endpoint, injecting faults from a scenario.
    Proxy(ProxyArgs),
    /// Proxy a live endpoint transparently and record traffic to a file.
    Record(RecordArgs),
    /// Serve recorded responses without an upstream, optionally with faults.
    Replay(ReplayArgs),
}

#[derive(Debug, Args)]
struct ListenArgs {
    /// Address to listen on.
    #[arg(
        long,
        env = "CHAINCHAOS_LISTEN",
        value_name = "ADDR",
        default_value = "127.0.0.1:9545"
    )]
    listen: SocketAddr,
}

#[derive(Debug, Args)]
struct ScenarioArgs {
    /// YAML file with fault rules and/or a timed scenario. Without it,
    /// chainchaos is fully transparent.
    #[arg(long, visible_alias = "config", value_name = "FILE")]
    scenario: Option<PathBuf>,

    /// Override the scenario's random seed.
    #[arg(long, value_name = "N")]
    seed: Option<u64>,
}

#[derive(Debug, Args)]
struct ProxyArgs {
    /// Upstream EVM JSON-RPC endpoint (http or https).
    #[arg(long, value_name = "URL")]
    upstream: Url,

    /// WebSocket upstream for eth_subscribe clients. Defaults to the
    /// upstream URL with ws/wss instead of http/https.
    #[arg(long, value_name = "URL")]
    upstream_ws: Option<Url>,

    #[command(flatten)]
    listen: ListenArgs,

    #[command(flatten)]
    scenario: ScenarioArgs,

    /// How long to wait for the upstream before answering with a gateway timeout.
    #[arg(long, value_name = "DURATION", default_value = "30s", value_parser = parse_duration)]
    upstream_timeout: Duration,
}

#[derive(Debug, Args)]
struct RecordArgs {
    /// Upstream EVM JSON-RPC endpoint (http or https).
    #[arg(long, value_name = "URL")]
    upstream: Url,

    /// Recording file to write (conventionally `.ccr`).
    #[arg(long, short, value_name = "FILE")]
    output: PathBuf,

    /// Replace the value of this JSON key wherever it appears in requests
    /// and responses. Repeatable.
    #[arg(long = "redact-field", value_name = "KEY")]
    redact_fields: Vec<String>,

    #[command(flatten)]
    listen: ListenArgs,

    /// How long to wait for the upstream before answering with a gateway timeout.
    #[arg(long, value_name = "DURATION", default_value = "30s", value_parser = parse_duration)]
    upstream_timeout: Duration,
}

#[derive(Debug, Args)]
struct ReplayArgs {
    /// Recording file produced by `chainchaos record`.
    recording: PathBuf,

    #[command(flatten)]
    listen: ListenArgs,

    #[command(flatten)]
    scenario: ScenarioArgs,

    /// Delay each response by its recorded upstream latency.
    #[arg(long)]
    replay_latency: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let result = match cli.command {
        Command::Proxy(args) => run_proxy(args).await,
        Command::Record(args) => run_record(args).await,
        Command::Replay(args) => run_replay(args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            error!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn load_scenario(args: &ScenarioArgs) -> Result<FaultConfig, String> {
    let mut config = match &args.scenario {
        Some(path) => FaultConfig::from_path(path).map_err(|e| e.to_string())?,
        None => FaultConfig::default(),
    };
    if let Some(seed) = args.seed {
        config.seed = seed;
    }
    Ok(config)
}

async fn run_proxy(args: ProxyArgs) -> Result<(), String> {
    let faults = load_scenario(&args.scenario)?;
    let upstream_ws = match args.upstream_ws {
        Some(url) => Some(url),
        None => derive_ws_url(&args.upstream),
    };
    info!(
        upstream = %redact(&args.upstream),
        upstream_ws = upstream_ws.as_ref().map(redact).unwrap_or_else(|| "-".into()),
        seed = faults.seed,
        fault_rules = faults.rules.len(),
        "mode: proxy"
    );
    let config = ProxyConfig {
        upstream: UpstreamConfig::Http {
            url: args.upstream,
            timeout: args.upstream_timeout,
        },
        upstream_ws,
        faults,
        record: None,
    };
    serve(config, args.listen.listen).await
}

async fn run_record(args: RecordArgs) -> Result<(), String> {
    info!(
        upstream = %redact(&args.upstream),
        output = %args.output.display(),
        redacted_fields = ?args.redact_fields,
        "mode: record"
    );
    let config = ProxyConfig {
        upstream: UpstreamConfig::Http {
            url: args.upstream.clone(),
            timeout: args.upstream_timeout,
        },
        upstream_ws: None,
        faults: FaultConfig::default(),
        record: Some(RecordConfig {
            output: args.output,
            redact_fields: args.redact_fields,
            upstream_label: redact(&args.upstream),
        }),
    };
    serve(config, args.listen.listen).await
}

async fn run_replay(args: ReplayArgs) -> Result<(), String> {
    let faults = load_scenario(&args.scenario)?;
    let recording = Recording::load(&args.recording).map_err(|e| e.to_string())?;
    info!(
        recording = %args.recording.display(),
        entries = recording.entries.len(),
        recorded_from = %recording.header.upstream,
        seed = faults.seed,
        fault_rules = faults.rules.len(),
        "mode: replay"
    );
    let config = ProxyConfig {
        upstream: UpstreamConfig::Replay {
            recording,
            replay_latency: args.replay_latency,
        },
        upstream_ws: None,
        faults,
        record: None,
    };
    serve(config, args.listen.listen).await
}

async fn serve(config: ProxyConfig, listen: SocketAddr) -> Result<(), String> {
    let rule_count = config.faults.rules.len();
    let proxy = Proxy::new(config).map_err(|e| e.to_string())?;
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("failed to listen on {listen}: {e}"))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| format!("failed to read listen address: {e}"))?;

    info!(listen = %local_addr, "chainchaos started");
    if rule_count == 0 {
        info!("no fault rules configured; running transparently");
    }
    proxy
        .serve(listener, shutdown_signal())
        .await
        .map_err(|e| e.to_string())?;
    info!("chainchaos stopped");
    Ok(())
}

/// `http://host:8545` -> `ws://host:8545` (Anvil and many nodes serve both
/// on one port; Geth uses a separate port, so pass --upstream-ws there).
fn derive_ws_url(http: &Url) -> Option<Url> {
    let scheme = match http.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => return None,
    };
    let mut ws = http.clone();
    ws.set_scheme(scheme).ok()?;
    Some(ws)
}

/// Hides credentials and API-key-looking paths (as used by hosted RPC
/// providers) when logging an upstream URL.
fn redact(url: &Url) -> String {
    let mut shown = url.clone();
    if !shown.username().is_empty() || shown.password().is_some() {
        let _ = shown.set_username("***");
        let _ = shown.set_password(None);
    }
    if shown.path() != "/" {
        shown.set_path("/***");
    }
    shown.set_query(None);
    shown.to_string()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("failed to listen for ctrl-c: {e}");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                error!("failed to listen for SIGTERM: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    info!("shutdown requested; draining in-flight requests");
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_proxy_args() {
        let cli = Cli::try_parse_from([
            "chainchaos",
            "proxy",
            "--upstream",
            "http://127.0.0.1:8545",
            "--upstream-timeout",
            "5s",
            "--config",
            "x.yaml",
            "--seed",
            "9",
        ])
        .unwrap();
        let Command::Proxy(args) = cli.command else {
            panic!("expected proxy");
        };
        assert_eq!(args.listen.listen, "127.0.0.1:9545".parse().unwrap());
        assert_eq!(args.upstream_timeout, Duration::from_secs(5));
        assert_eq!(args.scenario.scenario, Some(PathBuf::from("x.yaml")));
        assert_eq!(args.scenario.seed, Some(9));
    }

    #[test]
    fn parses_record_and_replay_args() {
        let cli = Cli::try_parse_from([
            "chainchaos",
            "record",
            "--upstream",
            "http://127.0.0.1:8545",
            "-o",
            "s.ccr",
            "--redact-field",
            "apiKey",
            "--redact-field",
            "token",
        ])
        .unwrap();
        let Command::Record(args) = cli.command else {
            panic!("expected record");
        };
        assert_eq!(args.redact_fields, ["apiKey", "token"]);

        let cli = Cli::try_parse_from([
            "chainchaos",
            "replay",
            "s.ccr",
            "--scenario",
            "scenarios/reorg.yaml",
            "--replay-latency",
        ])
        .unwrap();
        let Command::Replay(args) = cli.command else {
            panic!("expected replay");
        };
        assert!(args.replay_latency);
        assert_eq!(args.recording, PathBuf::from("s.ccr"));
    }

    #[test]
    fn derives_ws_urls() {
        let ws = derive_ws_url(&Url::parse("http://127.0.0.1:8545").unwrap()).unwrap();
        assert_eq!(ws.as_str(), "ws://127.0.0.1:8545/");
        let wss = derive_ws_url(&Url::parse("https://rpc.example.io/v3/key").unwrap()).unwrap();
        assert_eq!(wss.as_str(), "wss://rpc.example.io/v3/key");
    }

    #[test]
    fn redacts_upstream_secrets() {
        let url = Url::parse("https://user:pw@mainnet.example.io/v3/secretkey?k=v").unwrap();
        assert_eq!(redact(&url), "https://***@mainnet.example.io/***");
        let local = Url::parse("http://127.0.0.1:8545").unwrap();
        assert_eq!(redact(&local), "http://127.0.0.1:8545/");
    }
}

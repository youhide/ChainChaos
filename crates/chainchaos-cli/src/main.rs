use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use chainchaos_core::FaultConfig;
use chainchaos_core::duration::parse_duration;
use chainchaos_proxy::ProxyConfig;
use clap::{Parser, Subcommand};
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
    /// Run a transparent HTTP JSON-RPC proxy with optional fault injection.
    Proxy(ProxyArgs),
    // TODO(phase-6): Record, Replay
}

#[derive(Debug, clap::Args)]
struct ProxyArgs {
    /// Upstream EVM JSON-RPC endpoint (http or https).
    #[arg(long, value_name = "URL")]
    upstream: Url,

    /// Address to listen on.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:9545")]
    listen: SocketAddr,

    /// YAML file with fault rules. Without it, chainchaos is fully transparent.
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// How long to wait for the upstream before answering with a gateway timeout.
    #[arg(long, value_name = "DURATION", default_value = "30s", value_parser = parse_duration)]
    upstream_timeout: Duration,
    // TODO(phase-2): --scenario <FILE> and --seed <N>
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
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            error!("{message}");
            ExitCode::FAILURE
        }
    }
}

async fn run_proxy(args: ProxyArgs) -> Result<(), String> {
    let faults = match &args.config {
        Some(path) => FaultConfig::from_path(path).map_err(|e| e.to_string())?,
        None => FaultConfig::default(),
    };

    let rule_count = faults.rules.len();
    let upstream = redact(&args.upstream);
    let app = chainchaos_proxy::router(ProxyConfig {
        upstream: args.upstream,
        upstream_timeout: args.upstream_timeout,
        faults,
    })
    .map_err(|e| e.to_string())?;

    let listener = TcpListener::bind(args.listen)
        .await
        .map_err(|e| format!("failed to listen on {}: {e}", args.listen))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| format!("failed to read listen address: {e}"))?;

    info!(
        listen = %local_addr,
        %upstream,
        fault_rules = rule_count,
        "chainchaos proxy started"
    );
    if rule_count == 0 {
        info!("no fault rules configured; running as a transparent proxy");
    }

    chainchaos_proxy::serve(listener, app, shutdown_signal())
        .await
        .map_err(|e| e.to_string())?;
    info!("chainchaos proxy stopped");
    Ok(())
}

/// Hides credentials and API-key-looking paths (as used by hosted RPC
/// providers) when logging the upstream URL.
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
        ])
        .unwrap();
        let Command::Proxy(args) = cli.command;
        assert_eq!(args.listen, "127.0.0.1:9545".parse().unwrap());
        assert_eq!(args.upstream_timeout, Duration::from_secs(5));
    }

    #[test]
    fn redacts_upstream_secrets() {
        let url = Url::parse("https://user:pw@mainnet.example.io/v3/secretkey?k=v").unwrap();
        assert_eq!(redact(&url), "https://***@mainnet.example.io/***");
        let local = Url::parse("http://127.0.0.1:8545").unwrap();
        assert_eq!(redact(&local), "http://127.0.0.1:8545/");
    }
}

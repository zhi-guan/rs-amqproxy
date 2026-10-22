use amqproxy::config::Config;
use amqproxy::server::Server;
use tracing::{debug, error, info};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn init_logging(debug: bool) {
    let level = if debug {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_ansi(false)
        .with_target(false)
        .with_writer(std::io::stdout)
        .init();
}

#[tokio::main]
async fn main() {
    let config = Config::load();
    init_logging(config.debug);
    info!(
        upstream = %format!("{}:{}", config.upstream_host, config.upstream_port),
        tls = config.upstream_tls,
        listen = %format!("{}:{}", config.listen_address, config.listen_port),
        idle_timeout = %format!("{}s", config.idle_connection_timeout.as_secs()),
        "Proxy configured"
    );
    debug!("Debug logging enabled");
    let server = match Server::bind(config).await {
        Ok(s) => s,
        Err(e) => {
            error!("Cannot listen: {e}");
            std::process::exit(1);
        }
    };
    server.run().await;
}

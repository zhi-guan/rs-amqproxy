use std::time::Duration;

use clap::{ArgAction, Parser};
use url::Url;

/// 命令行参数，与 cloudamqp/amqproxy v0.4.4 兼容。
#[derive(Parser, Debug)]
#[command(
    name = "amqproxy",
    version,
    disable_version_flag = true,
    override_usage = "amqproxy [options] [amqp upstream url]"
)]
struct Args {
    /// Address to listen on
    #[arg(
        short = 'l',
        long = "listen",
        value_name = "ADDRESS",
        env = "LISTEN_ADDRESS",
        default_value = "localhost"
    )]
    listen: String,

    /// Port to listen on
    #[arg(
        short = 'p',
        long = "port",
        value_name = "PORT",
        env = "LISTEN_PORT",
        default_value_t = 5673
    )]
    port: u16,

    /// Verbose logging
    #[arg(short = 'd', long = "debug")]
    debug: bool,

    /// Seconds an unused pooled upstream connection stays open
    #[arg(
        short = 't',
        long = "idle-connection-timeout",
        value_name = "SECONDS",
        default_value_t = 5
    )]
    idle_connection_timeout: u64,

    /// Display version
    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue)]
    version: bool,

    /// Upstream AMQP URL, for example amqps://hostname
    #[arg(
        value_name = "UPSTREAM_URL",
        env = "AMQP_URL",
        required_unless_present = "version"
    )]
    upstream: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen_address: String,
    pub listen_port: u16,
    pub debug: bool,
    pub idle_connection_timeout: Duration,
    pub upstream_host: String,
    pub upstream_port: u16,
    pub upstream_tls: bool,
}

impl Config {
    /// 从进程的命令行和环境变量读取配置。出错时打印信息并退出。
    pub fn load() -> Config {
        let args = Args::parse();
        if args.version {
            // 与 v0.4.4 一样，只打印版本号。
            println!("{}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        Config::from_args(args)
    }

    fn from_args(args: Args) -> Config {
        let (upstream_host, upstream_port, upstream_tls) =
            match parse_upstream(args.upstream.as_deref().unwrap_or_default()) {
                Ok(v) => v,
                Err(msg) => {
                    eprintln!("{msg}");
                    std::process::exit(1);
                }
            };
        Config {
            listen_address: args.listen,
            listen_port: args.port,
            debug: args.debug,
            idle_connection_timeout: Duration::from_secs(args.idle_connection_timeout),
            upstream_host,
            upstream_port,
            upstream_tls,
        }
    }
}

/// 只使用 scheme、host 和 port；URL 中的用户名、密码和 vhost 被忽略。
fn parse_upstream(raw: &str) -> Result<(String, u16, bool), String> {
    const BAD_SCHEME: &str =
        "Not a valid upstream AMQP URL, should be on the format of amqps://hostname";
    let url = Url::parse(raw).map_err(|_| BAD_SCHEME.to_string())?;
    let default_port = match url.scheme() {
        "amqp" => 5672,
        "amqps" => 5671,
        _ => return Err(BAD_SCHEME.to_string()),
    };
    let host = match url.host_str() {
        Some(h) if !h.is_empty() => h.to_string(),
        _ => return Err("Invalid upstream URL".to_string()),
    };
    Ok((
        host,
        url.port().unwrap_or(default_port),
        url.scheme() == "amqps",
    ))
}

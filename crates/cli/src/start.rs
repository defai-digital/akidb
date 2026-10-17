//! `akidb start`: run a standalone server with no config file to write.
//!
//! The command renders the shipped Ollama profile (`config/standalone-ollama.toml`,
//! embedded in the binary) into the data directory with every path pointed
//! there, then starts the server in-process. A release archive therefore needs
//! only the `akidb` binary and a local embedding server to go from nothing to
//! `akidb index` and `akidb pack`.
//!
//! Everything stays loopback-first: the profile binds 127.0.0.1, and the
//! embedding endpoint must be loopback unless `--allow-remote-embeddings` is
//! passed.

use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;

const PROFILE: &str = include_str!("../../../config/standalone-ollama.toml");

/// Start a standalone AkiDB server with a generated local configuration.
#[derive(clap::Args, Debug)]
pub struct StartArgs {
    /// Directory for the database, WAL, auth token, and generated config.
    /// Defaults to `$AKIDB_HOME/data` or `~/.akidb/data`.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// gRPC port on 127.0.0.1.
    #[arg(long, default_value_t = 50051)]
    pub grpc_port: u16,

    /// OpenAI-compatible embeddings endpoint. The default is Ollama's.
    #[arg(long, default_value = "http://127.0.0.1:11434/v1/embeddings")]
    pub embedding_url: String,

    /// Embedding model name sent in the request.
    #[arg(long, default_value = "nomic-embed-text")]
    pub embedding_model: String,

    /// Embedding dimensions. `akidb index` must use the same value.
    #[arg(long, default_value_t = 768)]
    pub dimensions: usize,

    /// Allow a non-loopback embedding endpoint (text leaves this machine).
    #[arg(long, default_value_t = false)]
    pub allow_remote_embeddings: bool,

    /// Write the generated config and print the plan without starting.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

/// Directory used when `--data-dir` is not given.
fn default_data_dir() -> Result<PathBuf> {
    if let Some(home) = std::env::var_os("AKIDB_HOME").filter(|h| !h.is_empty()) {
        return Ok(PathBuf::from(home).join("data"));
    }
    let home = std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .context("HOME is not set; pass --data-dir")?;
    Ok(PathBuf::from(home).join(".akidb").join("data"))
}

fn set(table: &mut toml::Table, path: &[&str], value: toml::Value) -> Result<()> {
    let (last, parents) = path.split_last().expect("non-empty path");
    let mut current = table;
    for key in parents {
        current = current
            .get_mut(*key)
            .and_then(toml::Value::as_table_mut)
            .with_context(|| format!("profile has no [{}] table", parents.join(".")))?;
    }
    current.insert((*last).to_string(), value);
    Ok(())
}

/// Render the profile with paths under `data_dir` and the chosen embedding
/// settings. Pure, so it can be tested without starting a server.
pub fn render_config(data_dir: &Path, args: &StartArgs) -> Result<String> {
    if args.dimensions == 0 {
        bail!("--dimensions must be greater than zero");
    }
    // TOML integers are i64: casting a wider value would silently wrap into a
    // negative dimension in the generated config.
    let dimensions = i64::try_from(args.dimensions)
        .ok()
        .filter(|dimensions| *dimensions > 0)
        .context("--dimensions does not fit the configuration format")?;
    let mut table: toml::Table = PROFILE.parse().context("embedded profile is invalid")?;
    let path = |name: &str| toml::Value::String(data_dir.join(name).display().to_string());
    set(&mut table, &["auth", "token_file"], path("auth.token"))?;
    set(
        &mut table,
        &["memory", "rocksdb_path"],
        path("memory-rocksdb"),
    )?;
    set(&mut table, &["storage", "rocksdb_path"], path("rocksdb"))?;
    set(&mut table, &["storage", "wal_path"], path("wal"))?;
    set(
        &mut table,
        &["sql", "sqlite_path"],
        path("akidb-metadata.sqlite"),
    )?;
    set(
        &mut table,
        &["server", "grpc_port"],
        toml::Value::Integer(i64::from(args.grpc_port)),
    )?;
    set(
        &mut table,
        &["embedding", "url"],
        toml::Value::String(args.embedding_url.clone()),
    )?;
    set(
        &mut table,
        &["embedding", "model"],
        toml::Value::String(args.embedding_model.clone()),
    )?;
    set(
        &mut table,
        &["embedding", "dimensions"],
        toml::Value::Integer(dimensions),
    )?;
    set(
        &mut table,
        &["slo", "reference", "dimensions"],
        toml::Value::Integer(dimensions),
    )?;
    set(
        &mut table,
        &["embedding", "require_local_embeddings"],
        toml::Value::Boolean(!args.allow_remote_embeddings),
    )?;
    Ok(toml::to_string_pretty(&table)?)
}

/// `host:port` of an http(s) URL, or `None` when it cannot be read.
fn host_port(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit('@').next()?;
    if authority.is_empty() {
        return None;
    }
    let has_port = match authority.rfind(':') {
        Some(i) => !authority[i..].contains(']'),
        None => false,
    };
    if has_port {
        return Some(authority.to_string());
    }
    let default = if scheme == "https" { 443 } else { 80 };
    Some(format!("{authority}:{default}"))
}

fn embedding_reachable(url: &str) -> Option<bool> {
    let target = host_port(url)?;
    let addr = target.to_socket_addrs().ok()?.next()?;
    Some(TcpStream::connect_timeout(&addr, Duration::from_millis(750)).is_ok())
}

pub async fn run_start(args: StartArgs) -> Result<()> {
    let data_dir = match &args.data_dir {
        Some(dir) => dir.clone(),
        None => default_data_dir()?,
    };
    let rendered = render_config(&data_dir, &args)?;
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("cannot create {}", data_dir.display()))?;
    let config_path = data_dir.join("start.toml");
    std::fs::write(&config_path, &rendered)
        .with_context(|| format!("cannot write {}", config_path.display()))?;

    eprintln!("data directory : {}", data_dir.display());
    eprintln!("config         : {}", config_path.display());
    eprintln!("gRPC           : 127.0.0.1:{}", args.grpc_port);
    eprintln!(
        "embeddings     : {} ({}, {} dimensions)",
        args.embedding_url, args.embedding_model, args.dimensions
    );
    if args.dry_run {
        eprintln!("dry run: not starting the server");
        return Ok(());
    }

    // The server honours these environment overrides and they win over the
    // config written above, so the printed plan would otherwise disagree with
    // the process that actually starts.
    let overrides: Vec<&str> = [
        "AKIDB_LISTEN_ADDR",
        "AKIDB_DATA_DIR",
        "AKIDB_WAL_DIR",
        "AKIDB_DIMENSIONS",
        "AKIDB_METRICS_ADDR",
        "AKIDB_AUTH_MODE",
        "AKIDB_REQUIRE_LOCAL_EMBEDDINGS",
    ]
    .into_iter()
    .filter(|name| std::env::var_os(name).is_some())
    .collect();
    if !overrides.is_empty() {
        eprintln!(
            "warning: environment overrides take precedence over this config: {}",
            overrides.join(", ")
        );
    }

    if TcpListener::bind(("127.0.0.1", args.grpc_port)).is_err() {
        bail!(
            "127.0.0.1:{} is already in use. Stop the other process or pass --grpc-port",
            args.grpc_port
        );
    }
    if embedding_reachable(&args.embedding_url) == Some(false) {
        eprintln!(
            "warning: nothing is listening at {}. The server starts anyway, but `akidb index` \
             and text search will fail until the embedding server is up (for Ollama: \
             `ollama serve` and `ollama pull {}`).",
            args.embedding_url, args.embedding_model
        );
    }
    // `index` must use the same server address and embedding settings.
    let mut flags = format!("--server 127.0.0.1:{}", args.grpc_port);
    if args.embedding_url != "http://127.0.0.1:11434/v1/embeddings" {
        flags.push_str(&format!(" --embedding-url {}", args.embedding_url));
    }
    if args.embedding_model != "nomic-embed-text" {
        flags.push_str(&format!(" --embedding-model {}", args.embedding_model));
    }
    if args.dimensions != 768 {
        flags.push_str(&format!(" --dimensions {}", args.dimensions));
    }
    // `index` fails closed on a non-loopback embedding endpoint unless this is
    // passed, so the suggested command has to carry it or it cannot run.
    if args.allow_remote_embeddings {
        flags.push_str(" --allow-remote-embeddings");
    }
    eprintln!("\nnext: akidb index <folder> {flags}\n      akidb pack \"<question>\" --server 127.0.0.1:{}\n", args.grpc_port);

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        server: akidb_server::Args,
    }
    let config = config_path.display().to_string();
    let server = Wrap::parse_from([
        "akidb-server",
        "--config",
        config.as_str(),
        "--standalone",
        "--log-level",
        args.log_level.as_str(),
    ])
    .server;
    akidb_server::run(server)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> StartArgs {
        StartArgs {
            data_dir: None,
            grpc_port: 50123,
            embedding_url: "http://127.0.0.1:1234/v1/embeddings".into(),
            embedding_model: "bge-small".into(),
            dimensions: 384,
            allow_remote_embeddings: false,
            dry_run: false,
            log_level: "info".into(),
        }
    }

    #[test]
    fn rendered_config_points_every_path_at_the_data_dir() {
        let rendered = render_config(Path::new("/var/akidb"), &args()).unwrap();
        let t: toml::Table = rendered.parse().unwrap();
        assert_eq!(
            t["auth"]["token_file"].as_str(),
            Some("/var/akidb/auth.token")
        );
        assert_eq!(
            t["storage"]["rocksdb_path"].as_str(),
            Some("/var/akidb/rocksdb")
        );
        assert_eq!(t["storage"]["wal_path"].as_str(), Some("/var/akidb/wal"));
        assert_eq!(
            t["memory"]["rocksdb_path"].as_str(),
            Some("/var/akidb/memory-rocksdb")
        );
        assert!(!rendered.contains("./data"));
    }

    #[test]
    fn rendered_config_applies_embedding_settings_and_stays_loopback() {
        let t: toml::Table = render_config(Path::new("/d"), &args())
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(t["server"]["host"].as_str(), Some("127.0.0.1"));
        assert_eq!(t["server"]["grpc_port"].as_integer(), Some(50123));
        assert_eq!(t["embedding"]["model"].as_str(), Some("bge-small"));
        assert_eq!(t["embedding"]["dimensions"].as_integer(), Some(384));
        assert_eq!(t["slo"]["reference"]["dimensions"].as_integer(), Some(384));
        assert_eq!(
            t["embedding"]["require_local_embeddings"].as_bool(),
            Some(true)
        );
        // Memory stays off by default.
        assert_eq!(t["memory"]["enabled"].as_bool(), Some(false));
    }

    #[test]
    fn remote_embeddings_need_an_explicit_opt_in() {
        let mut a = args();
        a.allow_remote_embeddings = true;
        let t: toml::Table = render_config(Path::new("/d"), &a).unwrap().parse().unwrap();
        assert_eq!(
            t["embedding"]["require_local_embeddings"].as_bool(),
            Some(false)
        );
    }

    #[test]
    fn zero_dimensions_are_refused() {
        let mut a = args();
        a.dimensions = 0;
        assert!(render_config(Path::new("/d"), &a).is_err());
    }

    #[test]
    fn rendered_config_loads_as_a_server_config() {
        let rendered = render_config(Path::new("/d"), &args()).unwrap();
        toml::from_str::<akidb_common::config::AkiDbConfig>(&rendered).expect("server config");
    }

    #[test]
    fn host_port_parsing() {
        assert_eq!(
            host_port("http://127.0.0.1:11434/v1/embeddings").as_deref(),
            Some("127.0.0.1:11434")
        );
        assert_eq!(
            host_port("https://example.com/v1").as_deref(),
            Some("example.com:443")
        );
        assert_eq!(
            host_port("http://localhost/v1").as_deref(),
            Some("localhost:80")
        );
        assert_eq!(
            host_port("http://[::1]:8080/x").as_deref(),
            Some("[::1]:8080")
        );
        assert_eq!(host_port("http://[::1]/x").as_deref(), Some("[::1]:80"));
        assert_eq!(host_port("not a url"), None);
    }
}

//! `akidb index` and `akidb pack`: the shortest path from a folder of files
//! to a cited, token-budgeted context pack.
//!
//! `index` walks a folder, splits text and source files into line-aligned
//! chunks, embeds them through an OpenAI-compatible `/v1/embeddings` endpoint
//! (Ollama, LM Studio, llama.cpp, or the ax-engine sidecar), and inserts them
//! over gRPC with the metadata the citation contract reads (`source_uri`,
//! `document_version`, `content_hash`, byte offsets). `pack` issues a
//! `TextSearch` with `pack=true` and prints the typed `ContextPackV1`.
//!
//! Embedding egress fails closed: the endpoint must be loopback unless the
//! operator passes `--allow-remote-embeddings`.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use akidb_common::config::EmbeddingClientConfig;
use akidb_embedding::ax_engine::AxEngineEmbedding;
use akidb_embedding::EmbeddingService;
use akidb_proto::akidb_client::AkidbClient;
use akidb_proto::{
    DocumentChunk, GetDocumentRequest, GetDocumentResponse, ReplaceDocumentRequest,
    TextSearchRequest,
};
use anyhow::{bail, Context, Result};
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

use crate::chunking::{segment, CHUNKER_VERSION};
use crate::index_state::{default_state_dir, settings_fingerprint, FileEntry, IndexState};

/// File extensions treated as indexable text.
const TEXT_EXTENSIONS: &[&str] = &[
    "md", "markdown", "txt", "rst", "adoc", "org", "csv", "tsv", "json", "jsonl", "yaml", "yml",
    "toml", "ini", "cfg", "conf", "html", "htm", "xml", "rs", "py", "ts", "tsx", "js", "jsx",
    "mjs", "cjs", "go", "java", "kt", "swift", "c", "h", "cc", "cpp", "hpp", "cs", "rb", "php",
    "sh", "bash", "zsh", "sql", "proto", "gradle", "lua", "scala", "dart",
];

/// Connection settings shared by `index` and `pack`.
#[derive(clap::Args, Debug, Clone)]
pub struct ConnectArgs {
    /// AkiDB gRPC endpoint.
    #[arg(long, default_value = "127.0.0.1:50051")]
    pub server: String,

    /// Collection to write to or read from. Must match the server's active
    /// collection (normally `default`).
    #[arg(long, default_value = "default")]
    pub collection: String,

    /// PEM CA used to verify a TLS-enabled AkiDB server.
    #[arg(long)]
    pub tls_ca: Option<PathBuf>,

    /// Certificate DNS name override (defaults to the endpoint host).
    #[arg(long)]
    pub tls_domain: Option<String>,

    /// Connection and RPC timeout in seconds. `index` also uses it as the
    /// timeout for each embedding request.
    #[arg(long, default_value_t = 30)]
    pub timeout_seconds: u64,

    /// Allow plaintext gRPC to a non-loopback server. Document text and the
    /// bearer token would cross the network unencrypted.
    #[arg(long, default_value_t = false)]
    pub allow_plaintext_remote: bool,
}

/// Index a folder of text and source files.
#[derive(clap::Args, Debug)]
pub struct IndexArgs {
    /// Folder (or single file) to index.
    pub path: PathBuf,

    #[command(flatten)]
    pub connect: ConnectArgs,

    /// OpenAI-compatible embeddings endpoint. The default is Ollama's.
    #[arg(long, default_value = "http://127.0.0.1:11434/v1/embeddings")]
    pub embedding_url: String,

    /// Embedding model name sent in the request.
    #[arg(long, default_value = "nomic-embed-text")]
    pub embedding_model: String,

    /// Embedding dimensions. Must equal the server's index dimensions.
    #[arg(long, default_value_t = 768)]
    pub dimensions: usize,

    /// Allow a non-loopback embedding endpoint (text leaves this machine).
    #[arg(long, default_value_t = false)]
    pub allow_remote_embeddings: bool,

    /// Target maximum characters per chunk.
    #[arg(long, default_value_t = 1200)]
    pub chunk_chars: usize,

    /// Skip files larger than this many bytes.
    #[arg(long, default_value_t = 1_048_576)]
    pub max_file_bytes: u64,

    /// Chunks embedded per request.
    #[arg(long, default_value_t = 16)]
    pub batch_size: usize,

    /// Plan only: list files and chunk counts without embedding or writing.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Exclude a literal relative path or basename (repeatable). Also reads
    /// .akidbignore at the selected root. No globs or negation are accepted.
    #[arg(long)]
    pub exclude: Vec<String>,

    /// Re-embed every file even when its content has not changed since the
    /// last run. Files deleted from disk are still removed.
    #[arg(long, default_value_t = false)]
    pub full: bool,

    /// Split Rust, Python, TypeScript/JavaScript and Go files at symbol
    /// boundaries (default). Pass `--symbol-chunking=false` for plain
    /// line-aligned chunks.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub symbol_chunking: bool,

    /// Directory for the incremental-index state. Defaults to
    /// `$AKIDB_INDEX_STATE_DIR` or `~/.akidb/index-state`. The state is kept
    /// outside the indexed folder.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

/// Print a cited, token-budgeted context pack for a query.
#[derive(clap::Args, Debug)]
pub struct PackArgs {
    /// Natural-language query.
    pub query: String,

    #[command(flatten)]
    pub connect: ConnectArgs,

    /// Budget measured by the server counter, not a model-specific tokenizer.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..))]
    pub token_budget: u32,

    /// Number of candidate passages to retrieve before packing.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..))]
    pub top_k: u32,

    /// Retrieval mode override. Without it the server plans the mode and the
    /// request asks for hybrid dense plus lexical retrieval.
    #[arg(long, value_parser = ["auto", "vector", "bm25", "hybrid", "graph", "graph_hybrid"])]
    pub retrieval_mode: Option<String>,

    /// Print the typed pack as JSON instead of readable text.
    #[arg(long, default_value_t = false)]
    pub json: bool,

    /// Verify local citation bytes under this directory; never follows links
    /// or fetches URLs. Exits nonzero if any returned citation fails verification.
    #[arg(long)]
    pub verify_sources_under: Option<PathBuf>,
}

/// One chunk of a source file with its byte span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
    pub text: String,
    /// Symbol name(s) when the chunk follows a code symbol boundary.
    pub symbol: Option<String>,
}

/// Split `text` into line-aligned chunks of at most `max_chars` characters.
/// A single line longer than `max_chars` is split at character boundaries.
/// Whitespace-only chunks are dropped. Offsets are byte offsets into `text`.
pub fn chunk_text(text: &str, max_chars: usize) -> Vec<Chunk> {
    let max_chars = max_chars.max(1);
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut end = 0usize;
    let mut chars = 0usize;

    let flush = |start: usize, end: usize, chunks: &mut Vec<Chunk>| {
        if end > start {
            let slice = &text[start..end];
            if !slice.trim().is_empty() {
                chunks.push(Chunk {
                    start,
                    end,
                    text: slice.to_string(),
                    symbol: None,
                });
            }
        }
    };

    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let line_chars = line.chars().count();
        if line_chars > max_chars {
            flush(start, end, &mut chunks);
            // Split the oversized line at character boundaries.
            let mut piece_start = offset;
            let mut piece_chars = 0usize;
            for (i, _) in line.char_indices() {
                if piece_chars == max_chars {
                    flush(piece_start, offset + i, &mut chunks);
                    piece_start = offset + i;
                    piece_chars = 0;
                }
                piece_chars += 1;
            }
            flush(piece_start, offset + line.len(), &mut chunks);
            offset += line.len();
            start = offset;
            end = offset;
            chars = 0;
            continue;
        }
        if chars + line_chars > max_chars && end > start {
            flush(start, end, &mut chunks);
            start = end;
            chars = 0;
        }
        if chars == 0 {
            start = offset;
        }
        offset += line.len();
        end = offset;
        chars += line_chars;
    }
    flush(start, end, &mut chunks);
    chunks
}

pub(crate) fn is_indexable(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| TEXT_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
        .unwrap_or(false)
}

#[cfg(test)]
fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let base = if root.is_file() {
        root.parent().unwrap()
    } else {
        root
    };
    let rules = crate::index_filter::Exclusions::load(base, &[])?;
    let selection = crate::index_filter::collect(root, base, &rules)?;
    if root.is_file() && selection.files.is_empty() {
        bail!("explicit input is excluded or unsupported");
    }
    Ok(selection.files)
}

fn hex_prefix(bytes: &[u8], len: usize) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(len);
    for byte in digest.iter() {
        out.push_str(&format!("{byte:02x}"));
        if out.len() >= len {
            break;
        }
    }
    out.truncate(len);
    out
}

/// Deterministic chunk id: stable across re-indexing of the same file. The
/// caller passes the canonical absolute path, so two folders that contain a
/// file with the same relative name never share ids.
#[cfg(test)]
pub fn chunk_id(canonical_path: &str, index: usize) -> String {
    format!("{}-{index:05}", hex_prefix(canonical_path.as_bytes(), 16))
}

fn chunk_metadata(
    relative: &str,
    absolute: &Path,
    document_version: &str,
    chunk: &Chunk,
) -> Vec<u8> {
    let mut value = json!({
        "source_uri": url::Url::from_file_path(absolute).expect("canonical absolute path").to_string(),
        "file": relative,
        "document_id": hex_prefix(absolute.as_os_str().as_encoded_bytes(), 64),
        "document_version": document_version,
        "content_hash": hex_prefix(chunk.text.as_bytes(), 64),
        "start_offset": chunk.start,
        "end_offset": chunk.end,
        "ingest": "akidb-index",
    });
    if let Some(symbol) = &chunk.symbol {
        value["symbol"] = json!(symbol);
    }
    value.to_string().into_bytes()
}

pub(crate) async fn connect(args: &ConnectArgs) -> Result<AkidbClient<Channel>> {
    let tls_enabled = args.server.starts_with("https://") || args.tls_ca.is_some();
    let endpoint = if args.server.starts_with("http://") || args.server.starts_with("https://") {
        args.server.clone()
    } else if tls_enabled {
        format!("https://{}", args.server)
    } else {
        format!("http://{}", args.server)
    };
    let timeout = Duration::from_secs(args.timeout_seconds);
    let mut endpoint = Endpoint::from_shared(endpoint)?
        .connect_timeout(timeout)
        .timeout(timeout);
    if tls_enabled {
        let domain = args
            .tls_domain
            .clone()
            .or_else(|| endpoint.uri().host().map(str::to_string))
            .ok_or_else(|| anyhow::anyhow!("TLS endpoint has no certificate domain"))?;
        let mut tls = ClientTlsConfig::new().domain_name(domain);
        if let Some(ca_path) = &args.tls_ca {
            tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(ca_path)?));
        }
        endpoint = endpoint.tls_config(tls)?;
    } else if !host_is_loopback(endpoint.uri().host()) && !args.allow_plaintext_remote {
        bail!(
            "refusing plaintext gRPC to non-loopback server {}: document text and the bearer \
             token would be sent unencrypted. Use https:// or --tls-ca, or pass \
             --allow-plaintext-remote on a trusted network",
            args.server
        );
    }
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("cannot reach AkiDB at {}", args.server))?;
    Ok(AkidbClient::new(channel))
}

fn host_is_loopback(host: Option<&str>) -> bool {
    let Some(host) = host else { return false };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

fn explicit_token(token: Option<String>, file: Option<PathBuf>) -> Result<Option<String>> {
    if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
        return Ok(Some(token.trim().to_string()));
    }
    file.map(|path| {
        std::fs::read_to_string(path)
            .context("cannot read explicitly configured token file")
            .map(|token| token.trim().to_string())
    })
    .transpose()
    .map(|token| token.filter(|value| !value.is_empty()))
}

/// New onboarding commands use explicitly configured credentials. They do not
/// infer credentials from an unrelated database in the current directory.
pub(crate) fn authorize<T>(body: T) -> Result<Request<T>> {
    let mut request = Request::new(body);
    if let Some(token) = explicit_token(
        std::env::var("AKIDB_AUTH_TOKEN").ok(),
        std::env::var_os("AKIDB_AUTH_TOKEN_FILE").map(PathBuf::from),
    )? {
        let value: MetadataValue<Ascii> = format!("Bearer {token}").parse()?;
        request.metadata_mut().insert("authorization", value);
    }
    for (env, header) in [
        ("AKIDB_WORKSPACE", "x-akidb-workspace"),
        ("AKIDB_AGENT", "x-akidb-agent"),
    ] {
        if let Ok(value) = std::env::var(env) {
            let value = value.trim();
            if !value.is_empty() {
                request.metadata_mut().insert(header, value.parse()?);
            }
        }
    }
    Ok(request)
}

/// Owns the blocking embedding client and drops it on a blocking thread.
struct EmbedderHandle(Option<Arc<AxEngineEmbedding>>);

impl EmbedderHandle {
    fn arc(&self) -> Arc<AxEngineEmbedding> {
        Arc::clone(self.0.as_ref().expect("embedder present until drop"))
    }
}

impl Drop for EmbedderHandle {
    fn drop(&mut self) {
        if let Some(embedder) = self.0.take() {
            // Fire and forget: the runtime outlives this call.
            tokio::task::spawn_blocking(move || drop(embedder));
        }
    }
}

/// One file read, chunked, and ready to embed.
struct FilePlan {
    relative: String,
    /// Canonical absolute path; keys the chunk ids and the citation URI.
    absolute: PathBuf,
    document_version: String,
    chunks: Vec<Chunk>,
}

enum PlanOutcome {
    Ready(FilePlan),
    /// Oversized or not UTF-8. Read failures are reported separately.
    Skipped,
}

/// Read at most `max_bytes` of a file. Returns `None` when the file is larger
/// (checked while reading, so a file that grows after a metadata check cannot
/// exhaust memory) or is not valid UTF-8.
pub(crate) fn read_bounded_utf8(path: &Path, max_bytes: u64) -> Result<Option<String>> {
    let parent = path.parent().context("source has no parent directory")?;
    read_bounded_utf8_under(parent, path, max_bytes)
}

fn read_bounded_utf8_under(base: &Path, path: &Path, max_bytes: u64) -> Result<Option<String>> {
    let file = crate::citations::open_source(base, path.strip_prefix(base)?)?;
    if !file.metadata()?.is_file() {
        bail!("index input must be a regular file");
    }
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Ok(None);
    }
    Ok(String::from_utf8(bytes).ok())
}

fn plan_file(path: &Path, base: &Path, args: &IndexArgs) -> Result<PlanOutcome> {
    let Some(text) = read_bounded_utf8_under(base, path, args.max_file_bytes)
        .with_context(|| format!("cannot read {}", path.display()))?
    else {
        return Ok(PlanOutcome::Skipped);
    };
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();
    let chunks = segment(&text, extension, args.chunk_chars, args.symbol_chunking);
    let relative = path
        .strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    Ok(PlanOutcome::Ready(FilePlan {
        relative,
        absolute: path.to_path_buf(),
        document_version: hex_prefix(text.as_bytes(), 64),
        chunks,
    }))
}

pub async fn run_index(args: IndexArgs) -> Result<()> {
    if args.dimensions == 0 || args.batch_size == 0 || args.chunk_chars == 0 {
        bail!("--dimensions, --batch-size and --chunk-chars must be greater than zero");
    }
    if std::fs::symlink_metadata(&args.path)?
        .file_type()
        .is_symlink()
    {
        bail!("refusing a symlink indexing root; select the source directory explicitly");
    }
    let root = std::fs::canonicalize(&args.path)
        .with_context(|| format!("cannot resolve {}", args.path.display()))?;
    let base = if root.is_file() {
        root.parent().unwrap_or(&root).to_path_buf()
    } else {
        root.clone()
    };
    let rules = crate::index_filter::Exclusions::load(&base, &args.exclude)?;
    let selection = crate::index_filter::collect(&root, &base, &rules)?;
    if args.dry_run {
        for (path, reason) in &selection.excluded {
            println!(
                "{}\texcluded: {reason}",
                path.strip_prefix(&base).unwrap_or(path).display()
            );
        }
    }
    let files = selection.files;

    let settings = settings_fingerprint(
        &args.embedding_model,
        args.dimensions,
        args.chunk_chars,
        CHUNKER_VERSION,
        args.symbol_chunking,
    );
    let state_dir = args
        .state_dir
        .clone()
        .or_else(default_state_dir)
        .map(|dir| {
            // An explicit workspace gets separate pruning receipts. Preserve the
            // legacy default path for existing standalone users.
            match std::env::var("AKIDB_WORKSPACE")
                .ok()
                .filter(|w| !w.trim().is_empty())
            {
                Some(workspace) => dir.join(hex_prefix(workspace.trim().as_bytes(), 64)),
                None => dir,
            }
        });
    let mut state = match &state_dir {
        Some(dir) => Some(IndexState::load(
            dir,
            &args.connect.server,
            &args.connect.collection,
        )?),
        None => {
            eprintln!(
                "warning: no state directory (set HOME or --state-dir); \
                 every run re-embeds all files and removed files are not pruned"
            );
            None
        }
    };

    if args.dry_run {
        let mut planned = 0usize;
        let mut chunks = 0usize;
        let mut skipped = 0usize;
        let mut unchanged = 0usize;
        let mut present = std::collections::BTreeSet::new();
        for path in &files {
            match plan_file(path, &base, &args)? {
                PlanOutcome::Ready(plan) => {
                    let key = plan.absolute.to_string_lossy().to_string();
                    present.insert(key.clone());
                    let same = !args.full
                        && state
                            .as_ref()
                            .and_then(|s| s.get(&key))
                            .is_some_and(|e| entry_matches(e, &plan, &settings));
                    if same {
                        unchanged += 1;
                    }
                    println!(
                        "{}\t{} chunks{}",
                        plan.relative,
                        plan.chunks.len(),
                        if same {
                            "\t(unchanged since last index)"
                        } else {
                            ""
                        }
                    );
                    planned += 1;
                    chunks += plan.chunks.len();
                }
                PlanOutcome::Skipped => skipped += 1,
            }
        }
        let removed = state
            .as_ref()
            .map(|s| s.missing_under(&root, &present))
            .unwrap_or_default();
        for (file, entry) in &removed {
            println!(
                "{file}\twould remove {} chunks (file is gone or excluded)",
                entry.chunk_count
            );
        }
        eprintln!(
            "planned {planned} files, {chunks} chunks ({unchanged} unchanged, {} would be \
             removed, {skipped} files skipped as oversized or non-UTF-8)",
            removed.len()
        );
        if planned == 0 && removed.is_empty() {
            bail!("nothing to index under {}", args.path.display());
        }
        return Ok(());
    }
    if files.is_empty()
        && state
            .as_ref()
            .is_none_or(|s| s.missing_under(&root, &Default::default()).is_empty())
    {
        bail!("nothing to index under {}", args.path.display());
    }

    let config = EmbeddingClientConfig {
        enabled: true,
        url: args.embedding_url.clone(),
        model: args.embedding_model.clone(),
        dimensions: args.dimensions,
        timeout_ms: args.connect.timeout_seconds.saturating_mul(1000),
        max_batch_size: args.batch_size,
        require_local_embeddings: !args.allow_remote_embeddings,
    };
    config
        .validate_egress_policy()
        .map_err(|e| anyhow::anyhow!("{e} (pass --allow-remote-embeddings to override)"))?;
    // `reqwest::blocking` clients must be built and dropped off the async
    // runtime threads, so construction and teardown both use blocking tasks.
    let embedder = EmbedderHandle(Some(Arc::new(
        tokio::task::spawn_blocking(move || AxEngineEmbedding::new(config))
            .await?
            .map_err(|e| anyhow::anyhow!("embedding client: {e}"))?,
    )));
    let mut client = connect(&args.connect).await?;
    let info = client
        .get_collection(authorize(akidb_proto::GetCollectionRequest {
            name: args.connect.collection.clone(),
        })?)
        .await
        .context("cannot inspect collection; run akidb doctor")?
        .into_inner()
        .collection
        .context("collection not found; run akidb doctor")?;
    crate::doctor::validate_collection(&info, args.dimensions, &args.embedding_model)?;

    let mut inserted = 0usize;
    let mut indexed_files = 0usize;
    let mut unchanged = 0usize;
    let mut removed_files = 0usize;
    let mut skipped = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();
    // Paths that exist and were not skipped as empty or oversized. A file
    // that failed to read stays here so its earlier chunks are not pruned.
    let mut present = std::collections::BTreeSet::new();
    for path in &files {
        let plan = match plan_file(path, &base, &args) {
            Ok(PlanOutcome::Ready(plan)) => plan,
            Ok(PlanOutcome::Skipped) => {
                present.insert(path.to_string_lossy().to_string());
                skipped += 1;
                continue;
            }
            Err(e) => {
                present.insert(path.to_string_lossy().to_string());
                failures.push((path.display().to_string(), format!("{e:#}")));
                continue;
            }
        };
        let key = plan.absolute.to_string_lossy().to_string();
        present.insert(key.clone());
        if !args.full {
            if let Some(entry) = state.as_ref().and_then(|s| s.get(&key)) {
                if entry_matches(entry, &plan, &settings)
                    && server_has_file(&mut client, &args.connect.collection, &plan, &settings)
                        .await
                {
                    unchanged += 1;
                    continue;
                }
            }
        }
        match index_file(&mut client, &embedder, &plan, &args, &settings).await {
            Ok(count) => {
                inserted += count;
                indexed_files += 1;
                eprintln!("indexed {} ({count} chunks)", plan.relative);
                if let Some(state) = state.as_mut() {
                    state.set(
                        key,
                        FileEntry {
                            document_version: plan.document_version.clone(),
                            chunk_count: plan.chunks.len(),
                            settings: settings.clone(),
                        },
                    );
                }
            }
            Err(e) => {
                // Keep the previous local receipt. The server commits an entire
                // document or retains the old head; ambiguous replies are resolved
                // through GetDocument on the next attempt.
                failures.push((plan.relative.clone(), format!("{e:#}")));
            }
        }
    }

    // Remove chunks of files that were indexed before and are gone now.
    if let Some(state) = state.as_mut() {
        for (file, entry) in state.missing_under(&root, &present) {
            match delete_document(&mut client, &args.connect.collection, &file, &entry).await {
                Ok(deleted) => {
                    removed_files += 1;
                    eprintln!(
                        "removed {file} ({deleted} chunks; {} were recorded)",
                        entry.chunk_count
                    );
                    state.remove(&file);
                }
                Err(e) => failures.push((file, format!("{e:#}"))),
            }
        }
        if let Err(e) = state.save() {
            eprintln!("warning: could not save index state: {e:#}");
        }
    }

    println!(
        "indexed {inserted} chunks from {indexed_files} files into collection '{}' \
         ({unchanged} unchanged, {removed_files} removed, {skipped} skipped as empty, \
         oversized, or non-UTF-8)",
        args.connect.collection
    );
    if !failures.is_empty() {
        for (file, reason) in &failures {
            eprintln!("FAILED {file}: {reason}");
        }
        bail!(
            "{} file(s) failed. No delete/insert fallback was attempted. Check server health \
             and re-run `akidb index` to resolve the committed document state",
            failures.len()
        );
    }
    Ok(())
}

/// True when the recorded entry describes exactly this plan.
fn entry_matches(entry: &FileEntry, plan: &FilePlan, settings: &str) -> bool {
    entry.document_version == plan.document_version
        && entry.chunk_count == plan.chunks.len()
        && entry.settings == settings
}

fn document_id(path: &Path) -> String {
    hex_prefix(path.as_os_str().as_encoded_bytes(), 64)
}

async fn document_status(
    client: &mut AkidbClient<Channel>,
    collection: &str,
    path: &Path,
) -> Result<GetDocumentResponse> {
    Ok(client
        .get_document(authorize(GetDocumentRequest {
            collection: collection.to_string(),
            document_id: document_id(path),
        })?)
        .await
        .context(
            "GetDocument failed; atomic indexing requires a compatible mutable standalone server",
        )?
        .into_inner())
}

/// The server checks every durable chunk; local state alone is never sufficient.
async fn server_has_file(
    client: &mut AkidbClient<Channel>,
    collection: &str,
    plan: &FilePlan,
    settings: &str,
) -> bool {
    document_status(client, collection, &plan.absolute)
        .await
        .is_ok_and(|head| {
            head.found
                && head.complete
                && head.document_version == plan.document_version
                && head.chunk_count as usize == plan.chunks.len()
                && head.settings_fingerprint == hex_prefix(settings.as_bytes(), 64)
        })
}

fn replacement_body(
    collection: &str,
    path: &Path,
    version: &str,
    settings: &str,
    revision: String,
    chunks: Vec<DocumentChunk>,
) -> ReplaceDocumentRequest {
    let migrating = revision.is_empty();
    ReplaceDocumentRequest {
        collection: collection.into(),
        document_id: document_id(path),
        source_uri: url::Url::from_file_path(path)
            .expect("absolute path")
            .to_string(),
        document_version: version.into(),
        settings_fingerprint: hex_prefix(settings.as_bytes(), 64),
        expected_revision: revision,
        chunks,
        legacy_id_prefix: if migrating {
            format!("{}-", hex_prefix(path.to_string_lossy().as_bytes(), 16))
        } else {
            String::new()
        },
    }
}

async fn delete_document(
    client: &mut AkidbClient<Channel>,
    collection: &str,
    file: &str,
    entry: &FileEntry,
) -> Result<usize> {
    let path = Path::new(file);
    let head = document_status(client, collection, path).await?;
    if head.found && head.chunk_count > 0 && head.document_version != entry.document_version {
        bail!(
            "document changed since this index state was saved; refusing to prune a newer version"
        );
    }
    let body = replacement_body(
        collection,
        path,
        &hex_prefix(b"", 64),
        &entry.settings,
        head.revision,
        Vec::new(),
    );
    client
        .replace_document(authorize(body)?)
        .await
        .context("atomic document removal failed")?;
    Ok(if head.found {
        head.chunk_count as usize
    } else {
        entry.chunk_count
    })
}

/// Embed the complete bounded document before the server sees any mutation.
async fn index_file(
    client: &mut AkidbClient<Channel>,
    embedder: &EmbedderHandle,
    plan: &FilePlan,
    args: &IndexArgs,
    settings: &str,
) -> Result<usize> {
    use akidb_contracts::document::validate_document_bounds;
    validate_document_bounds(plan.chunks.len(), 0)?;
    let head = document_status(client, &args.connect.collection, &plan.absolute).await?;
    let mut chunks = Vec::with_capacity(plan.chunks.len());
    for batch in plan.chunks.chunks(args.batch_size) {
        let texts: Vec<String> = batch.iter().map(|c| c.text.clone()).collect();
        let embedder = embedder.arc();
        let vectors = tokio::task::spawn_blocking(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            embedder.embed_batch(&refs)
        })
        .await?
        .map_err(|e| anyhow::anyhow!("embedding failed before document replacement: {e}"))?;
        if vectors.len() != batch.len() {
            bail!("embedding server returned the wrong number of vectors");
        }
        for (chunk, vector) in batch.iter().zip(vectors) {
            chunks.push(DocumentChunk {
                vector,
                metadata: chunk_metadata(
                    &plan.relative,
                    &plan.absolute,
                    &plan.document_version,
                    chunk,
                ),
                text: chunk.text.clone(),
            });
        }
    }
    let body = replacement_body(
        &args.connect.collection,
        &plan.absolute,
        &plan.document_version,
        settings,
        head.revision,
        chunks,
    );
    validate_document_bounds(body.chunks.len(), body.encoded_len())?;
    let result = client
        .replace_document(authorize(body)?)
        .await
        .context("atomic replacement failed; retry after checking health (no legacy fallback)")?
        .into_inner();
    Ok(result.chunk_count as usize)
}

pub async fn run_pack(args: PackArgs) -> Result<()> {
    let verification_root = args
        .verify_sources_under
        .as_ref()
        .map(|p| {
            let path = p
                .canonicalize()
                .context("cannot resolve source verification directory")?;
            if !path.is_dir() {
                bail!("source verification root must be a directory");
            }
            Ok::<_, anyhow::Error>(path)
        })
        .transpose()?;
    let mut client = connect(&args.connect).await?;
    let body = TextSearchRequest {
        collection: args.connect.collection.clone(),
        text: args.query.clone(),
        top_k: args.top_k,
        hybrid: args.retrieval_mode.is_none(),
        pack: true,
        pack_token_budget: Some(args.token_budget),
        retrieval_mode: args.retrieval_mode.clone().unwrap_or_default(),
        ..Default::default()
    };
    let response = client
        .text_search(authorize(body)?)
        .await
        .map_err(|s| {
            anyhow::anyhow!(
                "TextSearch failed: {} ({}). TextSearch needs the server's [embedding] section \
                 enabled and pointed at the same model used by `akidb index`.",
                s.message(),
                s.code()
            )
        })?
        .into_inner();
    let Some(pack) = response.context_pack_v1 else {
        bail!("the server returned no typed context pack (is it an older build?)");
    };

    let verifications: Vec<_> = pack
        .items
        .iter()
        .map(|item| {
            verification_root
                .as_ref()
                .map(|root| crate::citations::verify(item, root))
                .unwrap_or("not_requested")
        })
        .collect();
    let verification_failed =
        verification_root.is_some() && verifications.iter().any(|s| *s != "verified");
    if args.json {
        let items: Vec<_> = pack
            .items
            .iter()
            .zip(&verifications)
            .map(|(item, verification)| {
                let c = item.citation.clone().unwrap_or_default();
                json!({
                    "chunk_id": item.chunk_id,
                    "score": item.score,
                    "reason": item.reason,
                    "text": item.text,
                    "source_verification": verification,
                    "citation": {
                        "chunk_id": c.chunk_id,
                        "source_uri": c.source_uri,
                        "source_version": c.source_version,
                        "generation_id": c.generation_id,
                        "document_id": c.document_id,
                        "document_version": c.document_version,
                        "content_hash": c.content_hash,
                        "start_offset": c.start_offset,
                        "end_offset": c.end_offset,
                        "citation_status": c.citation_status,
                    }
                })
            })
            .collect();
        let mut out = BTreeMap::new();
        out.insert("schema_version", json!(pack.schema_version));
        out.insert("text", json!(pack.text));
        out.insert(
            "token_count_is_estimate",
            json!((pack.token_counter == "conservative_v1").then_some(true)),
        );
        out.insert("token_budget", json!(pack.token_budget));
        out.insert("used_tokens", json!(pack.used_tokens));
        out.insert("token_counter", json!(pack.token_counter));
        out.insert("truncated", json!(pack.truncated));
        out.insert("candidate_limited", json!(pack.candidate_limited));
        out.insert("items", json!(items));
        println!("{}", serde_json::to_string_pretty(&out)?);
        if verification_failed {
            bail!("one or more local citations could not be verified; inspect source_verification");
        }
        return Ok(());
    }

    println!(
        "pack: {} passages, {}/{} tokens ({}), truncated={}, candidate_limited={}",
        pack.items.len(),
        pack.used_tokens,
        pack.token_budget,
        pack.token_counter,
        pack.truncated,
        pack.candidate_limited
    );
    if pack.token_counter == "conservative_v1" {
        println!("token count is an estimate, not the target model's tokenizer count");
    }
    for (n, item) in pack.items.iter().enumerate() {
        let c = item.citation.clone().unwrap_or_default();
        let source = if c.source_uri.is_empty() {
            "missing_source".to_string()
        } else {
            match (c.start_offset, c.end_offset) {
                (Some(s), Some(e)) => format!("{} [{s}..{e}]", c.source_uri),
                _ => c.source_uri.clone(),
            }
        };
        println!(
            "\n[{}] score={:.4} reason={} source={} verification={}\n{}",
            n + 1,
            item.score,
            item.reason,
            source,
            verifications[n],
            item.text.trim_end()
        );
    }
    if verification_failed {
        bail!("one or more local citations could not be verified");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_explicit_and_token_takes_precedence() {
        assert_eq!(explicit_token(None, None).unwrap(), None);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("credential.txt");
        assert!(explicit_token(None, Some(file.clone())).is_err());
        assert_eq!(
            explicit_token(Some(" synthetic-token ".into()), Some(file.clone()))
                .unwrap()
                .as_deref(),
            Some("synthetic-token")
        );
        std::fs::write(&file, " synthetic-file-token\n").unwrap();
        assert_eq!(
            explicit_token(None, Some(file)).unwrap().as_deref(),
            Some("synthetic-file-token")
        );
    }

    #[test]
    fn chunks_are_line_aligned_and_cover_the_text() {
        let text = "alpha\nbeta\ngamma\ndelta\n";
        let chunks = chunk_text(text, 12);
        assert!(chunks.len() > 1);
        let rebuilt: String = chunks.iter().map(|c| c.text.as_str()).collect();
        assert_eq!(rebuilt, text);
        for chunk in &chunks {
            assert_eq!(&text[chunk.start..chunk.end], chunk.text);
            assert!(chunk.text.chars().count() <= 12);
        }
    }

    #[test]
    fn oversized_line_is_split_at_char_boundaries() {
        let text = "é".repeat(25) + "\n";
        let chunks = chunk_text(&text, 10);
        assert!(chunks.len() >= 3);
        for chunk in &chunks {
            assert!(chunk.text.chars().count() <= 10);
            assert_eq!(&text[chunk.start..chunk.end], chunk.text);
        }
    }

    #[test]
    fn whitespace_only_input_yields_no_chunks() {
        assert!(chunk_text("   \n\n  \n", 100).is_empty());
        assert!(chunk_text("", 100).is_empty());
    }

    #[test]
    fn chunk_ids_are_deterministic_and_distinct() {
        assert_eq!(chunk_id("a/b.md", 3), chunk_id("a/b.md", 3));
        assert_ne!(chunk_id("a/b.md", 3), chunk_id("a/b.md", 4));
        assert_ne!(chunk_id("a/b.md", 3), chunk_id("a/c.md", 3));
        assert!(chunk_id("a/b.md", 3).ends_with("-00003"));
        assert_eq!(chunk_id("/x/a.md", 0).len(), 16 + 6);
    }

    #[test]
    fn collect_files_skips_hidden_dependency_and_non_text() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("keep.md"), "hello").unwrap();
        std::fs::write(root.join("image.png"), [0u8, 1, 2]).unwrap();
        std::fs::create_dir(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/config.toml"), "x").unwrap();
        std::fs::create_dir(root.join("node_modules")).unwrap();
        std::fs::write(root.join("node_modules/dep.js"), "x").unwrap();
        std::fs::create_dir(root.join("src")).unwrap();
        std::fs::write(root.join("src/lib.rs"), "fn main() {}").unwrap();
        let files = collect_files(root).unwrap();
        let names: Vec<_> = files
            .iter()
            .map(|p| p.strip_prefix(root).unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["keep.md", "src/lib.rs"]);
    }

    #[test]
    fn single_file_without_a_text_extension_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("id_rsa");
        std::fs::write(&key, "secret").unwrap();
        assert!(collect_files(&key).is_err());
        let note = dir.path().join("note.md");
        std::fs::write(&note, "hello").unwrap();
        assert_eq!(collect_files(&note).unwrap(), vec![note]);
    }

    #[test]
    fn bounded_read_rejects_large_and_non_utf8_files() {
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("big.txt");
        std::fs::write(&big, "x".repeat(100)).unwrap();
        assert_eq!(read_bounded_utf8(&big, 100).unwrap().unwrap().len(), 100);
        assert!(read_bounded_utf8(&big, 99).unwrap().is_none());
        let bin = dir.path().join("bin.txt");
        std::fs::write(&bin, [0xffu8, 0xfe, 0x00]).unwrap();
        assert!(read_bounded_utf8(&bin, 100).unwrap().is_none());
    }

    #[test]
    fn loopback_detection() {
        assert!(host_is_loopback(Some("127.0.0.1")));
        assert!(host_is_loopback(Some("localhost")));
        assert!(host_is_loopback(Some("[::1]")));
        assert!(!host_is_loopback(Some("10.0.0.5")));
        assert!(!host_is_loopback(Some("example.com")));
        assert!(!host_is_loopback(None));
    }

    #[test]
    fn metadata_carries_the_citation_keys() {
        let chunk = Chunk {
            start: 4,
            end: 9,
            text: "hello".to_string(),
            symbol: Some("greet".to_string()),
        };
        let bytes = chunk_metadata("docs/a.md", Path::new("/tmp/docs/a.md"), "v1", &chunk);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["source_uri"], "file:///tmp/docs/a.md");
        assert_eq!(value["document_version"], "v1");
        assert_eq!(value["start_offset"], 4);
        assert_eq!(value["end_offset"], 9);
        assert_eq!(value["content_hash"].as_str().unwrap().len(), 64);
    }
}

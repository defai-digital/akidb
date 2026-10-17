//! Content-free, manually invoked prerequisite diagnostics. The JSON report
//! contains only fixed check names/statuses and no paths, URLs or server errors.

use std::path::PathBuf;

use akidb_common::config::EmbeddingClientConfig;
use akidb_embedding::{ax_engine::AxEngineEmbedding, EmbeddingService};
use akidb_proto::{CollectionInfo, GetCollectionRequest, HealthRequest};
use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::index::{authorize, connect, ConnectArgs};

#[derive(clap::Args, Debug)]
pub struct DoctorArgs {
    #[command(flatten)]
    pub connect: ConnectArgs,
    /// Local OpenAI-compatible endpoint; receives only a fixed diagnostic phrase.
    #[arg(long, default_value = "http://127.0.0.1:11434/v1/embeddings")]
    pub embedding_url: String,
    #[arg(long, default_value = "nomic-embed-text")]
    pub embedding_model: String,
    #[arg(long, default_value_t = 768)]
    pub dimensions: usize,
    /// Allow the fixed embedding probe to leave this machine.
    #[arg(long)]
    pub allow_remote_embeddings: bool,
    /// Optionally inspect an existing data directory; never reads its contents.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Print a shareable content-free JSON report. Nothing is uploaded.
    #[arg(long)]
    pub json: bool,
}

pub fn validate_collection(info: &CollectionInfo, dimensions: usize, model: &str) -> Result<()> {
    if info.dimensions as usize != dimensions {
        bail!(
            "embedding dimension mismatch; use the server's dimensions or a fresh matching server"
        );
    }
    if !info.embedding_model_id.is_empty() && info.embedding_model_id != model {
        bail!(
            "embedding model mismatch; use the collection's bound model or a fresh matching server"
        );
    }
    Ok(())
}

fn check(checks: &mut Vec<Value>, name: &str, status: &str, action: &str) {
    checks.push(json!({"check": name, "status": status, "action": action}));
}

fn report(checks: Vec<Value>) -> Value {
    let ok = checks.iter().all(|c| c["status"] != "failed");
    let all_verified = ok && checks.iter().all(|c| c["status"] == "passed");
    json!({
        "schema_version": "akidb.doctor.v1", "cli_version": env!("CARGO_PKG_VERSION"),
        "ok": ok, "all_verified": all_verified, "checks": checks,
    })
}

pub async fn run(args: DoctorArgs) -> Result<()> {
    if args.dimensions == 0 || args.connect.timeout_seconds == 0 {
        bail!("--dimensions and --timeout-seconds must be greater than zero");
    }
    let mut checks = Vec::new();
    match connect(&args.connect).await {
        Ok(mut client) => {
            match client.health(authorize(HealthRequest {})?).await {
                Ok(r) if r.get_ref().healthy && r.get_ref().ready =>
                    check(&mut checks, "server_readiness", "passed", ""),
                _ => check(&mut checks, "server_readiness", "failed", "Check server readiness and authentication with akidb health --require-ready."),
            }
            match client.get_collection(authorize(GetCollectionRequest { name: args.connect.collection.clone() })?).await {
                Ok(r) if r.get_ref().found => match r.into_inner().collection {
                    Some(info) => {
                        let valid = validate_collection(&info, args.dimensions, &args.embedding_model).is_ok();
                        check(&mut checks, "collection_compatibility", if valid { "passed" } else { "failed" },
                            if valid { "" } else { "Match the collection dimensions and embedding model; do not mix embedding spaces." });
                        if info.embedding_model_id.is_empty() {
                            check(&mut checks, "embedding_model_binding", "unverified", "The server does not declare a model binding. Confirm matching start/index model settings manually.");
                        }
                    }
                    None => check(&mut checks, "collection_compatibility", "failed", "Select an existing active collection."),
                },
                _ => check(&mut checks, "collection_compatibility", "failed", "Check collection access and server version."),
            }
        }
        Err(_) => check(&mut checks, "server_connection", "failed", "Start AkiDB; check endpoint, TLS and authentication settings. Plaintext remote connections require explicit opt-in."),
    }
    let config = EmbeddingClientConfig {
        enabled: true,
        url: args.embedding_url,
        model: args.embedding_model,
        dimensions: args.dimensions,
        timeout_ms: args.connect.timeout_seconds.saturating_mul(1000),
        max_batch_size: 1,
        require_local_embeddings: !args.allow_remote_embeddings,
    };
    if config.validate_egress_policy().is_err() {
        check(
            &mut checks,
            "embedding_probe",
            "failed",
            "Use a loopback embedding endpoint or explicitly allow remote embeddings.",
        );
    } else {
        let probe = tokio::task::spawn_blocking(move || {
            let client = AxEngineEmbedding::new(config)?;
            client.embed("AkiDB diagnostic probe")
        })
        .await;
        let success = matches!(probe, Ok(Ok(_)));
        check(
            &mut checks,
            "embedding_probe",
            if success { "passed" } else { "failed" },
            if success {
                ""
            } else {
                "Start the embedding service, install the requested model, and check its output dimensions."
            },
        );
    }
    if let Some(path) = args.data_dir {
        let usable =
            std::fs::metadata(path).is_ok_and(|m| m.is_dir() && !m.permissions().readonly());
        check(
            &mut checks,
            "data_directory_metadata",
            if usable { "passed" } else { "failed" },
            if usable {
                "Metadata only; no write or free-space probe was performed."
            } else {
                "Create an accessible writable data directory or choose another path."
            },
        );
    }
    let result = report(checks);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        for c in result["checks"].as_array().expect("checks array") {
            println!(
                "{}: {} {}",
                c["check"].as_str().unwrap(),
                c["status"].as_str().unwrap(),
                c["action"].as_str().unwrap()
            );
        }
    }
    if result["ok"] != true {
        bail!("prerequisite checks failed; follow the actions in the diagnostic report");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_and_dimension_must_match_the_collection() {
        let info = CollectionInfo {
            dimensions: 8,
            embedding_model_id: "local-model".into(),
            ..Default::default()
        };
        assert!(validate_collection(&info, 8, "local-model").is_ok());
        assert!(validate_collection(&info, 4, "local-model").is_err());
        assert!(validate_collection(&info, 8, "different-model").is_err());
    }

    #[test]
    fn unknown_checks_do_not_claim_full_verification_and_failures_are_nonzero() {
        let mut checks = Vec::new();
        check(
            &mut checks,
            "embedding_model_binding",
            "unverified",
            "Confirm manually.",
        );
        let result = report(checks.clone());
        assert_eq!(result["ok"], true);
        assert_eq!(result["all_verified"], false);
        check(
            &mut checks,
            "server_readiness",
            "failed",
            "Start the server.",
        );
        assert_eq!(report(checks)["ok"], false);
    }
}

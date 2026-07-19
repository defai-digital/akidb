//! Data-plane authentication (ADR-0002.2 / GAP-001).
//!
//! Bearer-token interceptor plus token bootstrap helpers used by the server
//! binary. Auth context (workspace) is attached as a request extension so
//! handlers can enforce workspace ACL without re-parsing metadata.

use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use akidb_common::config::{AuthConfig, AuthMode};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
use tracing::{info, warn};
use uuid::Uuid;

/// Metadata keys accepted by the data plane.
pub const AUTH_HEADER: &str = "authorization";
pub const WORKSPACE_HEADER: &str = "x-akidb-workspace";
pub const AGENT_HEADER: &str = "x-akidb-agent";

/// Request-scoped identity after auth succeeds (or is disabled).
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub workspace_id: String,
    pub agent_id: Option<String>,
    pub authenticated: bool,
}

/// Resolved auth material for the running process.
#[derive(Debug, Clone)]
pub struct AuthRuntime {
    pub config: AuthConfig,
    pub token: Option<String>,
    pub bind_is_loopback: bool,
}

impl AuthRuntime {
    /// Load or create a token according to config and bind address policy.
    pub fn bootstrap(config: AuthConfig, bind_host: &str) -> Result<Self, String> {
        let bind_is_loopback = is_loopback_host(bind_host);
        let required = match config.mode {
            AuthMode::Disabled => false,
            AuthMode::Required => true,
            AuthMode::LoopbackOptional => !bind_is_loopback,
        };

        if !bind_is_loopback && config.mode == AuthMode::LoopbackOptional {
            info!(
                host = %bind_host,
                "non-loopback bind: bearer auth is required (auth.mode=loopback_optional)"
            );
        }

        let token = if matches!(config.mode, AuthMode::Disabled) {
            None
        } else {
            Some(resolve_or_create_token(&config)?)
        };

        if required && token.as_ref().map(|t| t.is_empty()).unwrap_or(true) {
            return Err(
                "authentication token is required for this bind/auth.mode but none is configured"
                    .to_string(),
            );
        }

        if !bind_is_loopback && token.is_none() && !matches!(config.mode, AuthMode::Disabled) {
            return Err(
                "refusing to bind non-loopback without an auth token; set auth.token or auth.token_file"
                    .to_string(),
            );
        }

        Ok(Self {
            config,
            token,
            bind_is_loopback,
        })
    }

    pub fn token_required(&self) -> bool {
        match self.config.mode {
            AuthMode::Disabled => false,
            AuthMode::Required => true,
            AuthMode::LoopbackOptional => !self.bind_is_loopback,
        }
    }

    /// Validate incoming metadata and build an [`AuthContext`].
    pub fn authorize(&self, metadata: &MetadataMap) -> Result<AuthContext, Status> {
        let presented = extract_bearer(metadata);
        let required = self.token_required();

        let authenticated = match (&self.token, presented.as_deref()) {
            (Some(expected), Some(got))
                if constant_time_eq(expected.as_bytes(), got.as_bytes()) =>
            {
                true
            }
            (Some(_), Some(_)) => {
                return Err(Status::unauthenticated("invalid bearer token"));
            }
            (Some(_), None) if required => {
                return Err(Status::unauthenticated(
                    "missing authorization bearer token",
                ));
            }
            (Some(_), None) => false,
            (None, _) => !required,
        };

        if required && !authenticated {
            return Err(Status::unauthenticated(
                "authentication required for this endpoint",
            ));
        }

        let workspace_id = metadata
            .get(WORKSPACE_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(self.config.acl.default_workspace.as_str())
            .to_string();

        let agent_id = metadata
            .get(AGENT_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        Ok(AuthContext {
            workspace_id,
            agent_id,
            authenticated,
        })
    }
}

/// tonic interceptor that enforces bearer auth and injects [`AuthContext`].
#[derive(Clone)]
pub struct AuthInterceptor {
    runtime: AuthRuntime,
}

impl AuthInterceptor {
    pub fn new(runtime: AuthRuntime) -> Self {
        Self { runtime }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let ctx = self.runtime.authorize(request.metadata())?;
        request.extensions_mut().insert(ctx);
        Ok(request)
    }
}

/// Extract [`AuthContext`] from a typed request (after interceptor).
pub fn auth_context<T>(request: &Request<T>) -> AuthContext {
    request
        .extensions()
        .get::<AuthContext>()
        .cloned()
        .unwrap_or(AuthContext {
            workspace_id: "default".to_string(),
            agent_id: None,
            authenticated: false,
        })
}

fn extract_bearer(metadata: &MetadataMap) -> Option<String> {
    let raw = metadata.get(AUTH_HEADER)?.to_str().ok()?.trim();
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .unwrap_or(raw)
        .trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn resolve_or_create_token(config: &AuthConfig) -> Result<String, String> {
    if let Some(token) = config.token.as_ref().map(|t| t.trim().to_string()) {
        if !token.is_empty() {
            return Ok(token);
        }
    }
    if let Ok(env) = std::env::var("AKIDB_AUTH_TOKEN") {
        let env = env.trim().to_string();
        if !env.is_empty() {
            return Ok(env);
        }
    }

    let path = PathBuf::from(&config.token_file);
    if path.exists() {
        let token = fs::read_to_string(&path)
            .map_err(|e| format!("failed to read auth token file {}: {e}", path.display()))?
            .trim()
            .to_string();
        if token.is_empty() {
            return Err(format!("auth token file {} is empty", path.display()));
        }
        return Ok(token);
    }

    let token = format!("akidb_{}", Uuid::new_v4().simple());
    write_token_file(&path, &token)?;
    info!(
        path = %path.display(),
        "generated new auth token file (store securely; also available as AKIDB_AUTH_TOKEN)"
    );
    // Print once for operator bootstrap (MCP/gRPC clients).
    eprintln!("AkiDB auth token (save this): {token}");
    Ok(token)
}

fn write_token_file(path: &Path, token: &str) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create token directory {}: {e}", parent.display()))?;
    }
    fs::write(path, format!("{token}\n"))
        .map_err(|e| format!("failed to write token file {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        if let Err(e) = fs::set_permissions(path, perms) {
            warn!(path = %path.display(), error = %e, "failed to set token file mode 0600");
        }
    }
    Ok(())
}

pub fn is_loopback_host(host: &str) -> bool {
    let host = host.trim();
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_common::config::AclConfig;
    use tonic::metadata::MetadataValue;

    fn runtime(mode: AuthMode, token: Option<&str>, loopback: bool) -> AuthRuntime {
        AuthRuntime {
            config: AuthConfig {
                mode,
                token_file: "./data/auth.token".into(),
                token: token.map(str::to_string),
                acl: AclConfig::default(),
            },
            token: token.map(str::to_string),
            bind_is_loopback: loopback,
        }
    }

    #[test]
    fn loopback_optional_allows_missing_token_on_loopback() {
        let rt = runtime(AuthMode::LoopbackOptional, Some("secret"), true);
        let meta = MetadataMap::new();
        let ctx = rt.authorize(&meta).unwrap();
        assert!(!ctx.authenticated);
        assert_eq!(ctx.workspace_id, "default");
    }

    #[test]
    fn loopback_optional_requires_token_off_loopback() {
        let rt = runtime(AuthMode::LoopbackOptional, Some("secret"), false);
        let meta = MetadataMap::new();
        let err = rt.authorize(&meta).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn accepts_valid_bearer_and_workspace_header() {
        let rt = runtime(AuthMode::Required, Some("secret"), true);
        let mut meta = MetadataMap::new();
        meta.insert(AUTH_HEADER, MetadataValue::from_static("Bearer secret"));
        meta.insert(WORKSPACE_HEADER, MetadataValue::from_static("team-a"));
        let ctx = rt.authorize(&meta).unwrap();
        assert!(ctx.authenticated);
        assert_eq!(ctx.workspace_id, "team-a");
    }

    #[test]
    fn rejects_invalid_token() {
        let rt = runtime(AuthMode::Required, Some("secret"), true);
        let mut meta = MetadataMap::new();
        meta.insert(AUTH_HEADER, MetadataValue::from_static("Bearer wrong"));
        assert!(rt.authorize(&meta).is_err());
    }

    #[test]
    fn detects_loopback_hosts() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("192.168.1.10"));
    }
}

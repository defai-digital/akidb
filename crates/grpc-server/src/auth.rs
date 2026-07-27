//! Data-plane authentication and principal-derived Memory authorization.
//!
//! The legacy vector data plane retains its workspace ACL compatibility
//! surface. Authoritative Memory uses the stricter ADR-0006 path: a credential
//! binds to one principal and versioned grants, while request workspace,
//! namespace, purpose, capability, and delegated agent can only narrow those
//! grants.

use std::collections::HashSet;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use akidb_common::config::{
    AuthConfig, AuthMode, PrincipalConfig, PrincipalCredentialConfig, PrincipalKind,
};
use akidb_contracts::Sensitivity;
use akidb_storage::{
    MemoryAccessGrant, MemoryAccessIssuer, MemoryAccessProof, MemoryAccessVerifier,
};
use sha2::{Digest, Sha256};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
use tracing::info;
use uuid::Uuid;

/// Metadata keys accepted by the data plane.
pub const AUTH_HEADER: &str = "authorization";
pub const WORKSPACE_HEADER: &str = "x-akidb-workspace";
pub const AGENT_HEADER: &str = "x-akidb-agent";

const MEMORY_CAPABILITIES: &[&str] = &[
    "memory.observe",
    "memory.propose",
    "memory.remember",
    "memory.read",
    "memory.recall",
    "memory.correct",
    "memory.retract",
    "memory.forget",
    "memory.history",
    "memory.export",
    "memory.replay",
    "memory.delete.plan",
    "memory.delete.execute",
    "memory.admin",
];
const MAX_AUTH_VALUE_BYTES: usize = 1_024;
const MIN_PRINCIPAL_TOKEN_BYTES: usize = 16;

/// Legacy request-scoped identity used by the existing vector/management data
/// plane. Memory handlers must use [`memory_auth_context`] instead.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub workspace_id: String,
    pub agent_id: Option<String>,
    pub authenticated: bool,
}

/// Maximum Memory authority derived from a credential and versioned grants.
/// Fields are private so handlers must use the narrowing authorization method.
#[derive(Debug, Clone)]
pub struct MemoryAuthContext {
    principal_id: String,
    principal_kind: PrincipalKind,
    credential_id: String,
    authenticated: bool,
    eligible: bool,
    denial_reason: &'static str,
    authorization_epoch: u64,
    grant_version: u64,
    process_workspace_id: String,
    workspaces: Vec<String>,
    namespaces: Vec<String>,
    agent_ids: Vec<String>,
    allow_shared_memory: bool,
    entity_keys: Vec<String>,
    data_subject_ids: Vec<String>,
    session_ids: Vec<String>,
    task_ids: Vec<String>,
    sensitivities: Vec<Sensitivity>,
    purposes: Vec<String>,
    capabilities: Vec<String>,
    access_issuer: MemoryAccessIssuer,
}

/// Optional exact request selectors that narrow a principal's signed record
/// scope ceiling. Empty vectors inherit the grant; request wildcards are
/// rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryScopeSelector {
    pub entity_keys: Vec<String>,
    pub data_subject_ids: Vec<String>,
    pub session_ids: Vec<String>,
    pub task_ids: Vec<String>,
    pub maximum_sensitivity: Option<Sensitivity>,
}

/// Scope/capability proof produced only after request values have been
/// intersected with a [`MemoryAuthContext`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedMemoryContext {
    principal_id: String,
    principal_kind: PrincipalKind,
    credential_id: String,
    workspace_id: String,
    namespace: String,
    request_purpose: String,
    delegated_agent_id: Option<String>,
    capability: String,
    authorization_epoch: u64,
    grant_version: u64,
    authorization_decision_id: String,
    access_proof: MemoryAccessProof,
}

impl AuthorizedMemoryContext {
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub fn principal_kind(&self) -> PrincipalKind {
        self.principal_kind
    }

    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn request_purpose(&self) -> &str {
        &self.request_purpose
    }

    pub fn delegated_agent_id(&self) -> Option<&str> {
        self.delegated_agent_id.as_deref()
    }

    pub fn capability(&self) -> &str {
        &self.capability
    }

    pub fn authorization_epoch(&self) -> u64 {
        self.authorization_epoch
    }

    pub fn grant_version(&self) -> u64 {
        self.grant_version
    }

    pub fn authorization_decision_id(&self) -> &str {
        &self.authorization_decision_id
    }

    /// Signed, narrowed proof accepted by the canonical storage boundary.
    pub fn storage_proof(&self) -> &MemoryAccessProof {
        &self.access_proof
    }
}

impl MemoryAuthContext {
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub fn authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn authorization_epoch(&self) -> u64 {
        self.authorization_epoch
    }

    pub fn grant_version(&self) -> u64 {
        self.grant_version
    }

    /// Intersect request selectors with credential-derived maximum grants.
    pub fn authorize_scope(
        &self,
        workspace_id: &str,
        namespace: &str,
        request_purpose: &str,
        delegated_agent_id: Option<&str>,
        capability: &str,
    ) -> Result<AuthorizedMemoryContext, Status> {
        self.authorize_scoped(
            workspace_id,
            namespace,
            request_purpose,
            delegated_agent_id,
            &MemoryScopeSelector::default(),
            capability,
        )
    }

    pub fn authorize_scoped(
        &self,
        workspace_id: &str,
        namespace: &str,
        request_purpose: &str,
        delegated_agent_id: Option<&str>,
        scope: &MemoryScopeSelector,
        capability: &str,
    ) -> Result<AuthorizedMemoryContext, Status> {
        if !self.eligible {
            return Err(Status::permission_denied(self.denial_reason));
        }
        validate_request_selector("workspace_id", workspace_id)?;
        validate_request_selector("namespace", namespace)?;
        validate_request_selector("request_purpose", request_purpose)?;
        validate_request_selector("capability", capability)?;
        if let Some(agent_id) = delegated_agent_id {
            validate_request_selector("delegated_agent_id", agent_id)?;
        }

        if workspace_id != self.process_workspace_id
            || !self
                .workspaces
                .iter()
                .any(|workspace| workspace == workspace_id)
        {
            return Err(Status::permission_denied(
                "requested Memory workspace is not granted to this principal",
            ));
        }
        if !self
            .namespaces
            .iter()
            .any(|grant| namespace_matches(grant, namespace))
        {
            return Err(Status::permission_denied(
                "requested Memory namespace is not granted to this principal",
            ));
        }
        if !self
            .purposes
            .iter()
            .any(|grant| grant == "**" || grant == request_purpose)
        {
            return Err(Status::permission_denied(
                "requested Memory purpose is not granted to this principal",
            ));
        }
        if !self
            .capabilities
            .iter()
            .any(|granted| granted == capability)
        {
            return Err(Status::permission_denied(
                "required Memory capability is not granted to this principal",
            ));
        }
        if let Some(agent_id) = delegated_agent_id {
            if !self
                .agent_ids
                .iter()
                .any(|granted| granted == "**" || granted == agent_id)
            {
                return Err(Status::permission_denied(
                    "delegated agent is not granted to this principal",
                ));
            }
        }

        let entity_keys =
            effective_scope_values("entity_keys", &self.entity_keys, &scope.entity_keys)?;
        if entity_keys.is_empty() {
            return Err(Status::permission_denied(
                "principal has no effective Memory entity grant",
            ));
        }
        let data_subject_ids = effective_scope_values(
            "data_subject_ids",
            &self.data_subject_ids,
            &scope.data_subject_ids,
        )?;
        let session_ids =
            effective_scope_values("session_ids", &self.session_ids, &scope.session_ids)?;
        let task_ids = effective_scope_values("task_ids", &self.task_ids, &scope.task_ids)?;
        let sensitivities =
            effective_sensitivities(&self.sensitivities, scope.maximum_sensitivity)?;

        let authorization_decision_id = format!("authz_{}", Uuid::now_v7().simple());
        let access_proof = self
            .access_issuer
            .issue(MemoryAccessGrant {
                principal_id: self.principal_id.clone(),
                credential_id: self.credential_id.clone(),
                workspace_id: workspace_id.to_string(),
                namespace: namespace.to_string(),
                request_purpose: request_purpose.to_string(),
                delegated_agent_id: delegated_agent_id.map(str::to_string),
                allow_shared_memory: self.allow_shared_memory,
                entity_keys,
                data_subject_ids,
                require_data_subject: !scope.data_subject_ids.is_empty(),
                session_ids,
                require_session: !scope.session_ids.is_empty(),
                task_ids,
                require_task: !scope.task_ids.is_empty(),
                sensitivities,
                capability: capability.to_string(),
                authorization_epoch: self.authorization_epoch,
                grant_version: self.grant_version,
                authorization_decision_id: authorization_decision_id.clone(),
                system_job: false,
            })
            .map_err(|_| Status::internal("failed to issue Memory access proof"))?;

        Ok(AuthorizedMemoryContext {
            principal_id: self.principal_id.clone(),
            principal_kind: self.principal_kind,
            credential_id: self.credential_id.clone(),
            workspace_id: workspace_id.to_string(),
            namespace: namespace.to_string(),
            request_purpose: request_purpose.to_string(),
            delegated_agent_id: delegated_agent_id.map(str::to_string),
            capability: capability.to_string(),
            authorization_epoch: self.authorization_epoch,
            grant_version: self.grant_version,
            authorization_decision_id,
            access_proof,
        })
    }

    fn denied(
        principal_id: impl Into<String>,
        credential_id: impl Into<String>,
        authenticated: bool,
        config: &AuthConfig,
        access_issuer: MemoryAccessIssuer,
        reason: &'static str,
    ) -> Self {
        Self {
            principal_id: principal_id.into(),
            principal_kind: PrincipalKind::Service,
            credential_id: credential_id.into(),
            authenticated,
            eligible: false,
            denial_reason: reason,
            authorization_epoch: config.authorization_epoch,
            grant_version: 0,
            process_workspace_id: memory_workspace(config),
            workspaces: Vec::new(),
            namespaces: Vec::new(),
            agent_ids: Vec::new(),
            allow_shared_memory: false,
            entity_keys: Vec::new(),
            data_subject_ids: Vec::new(),
            session_ids: Vec::new(),
            task_ids: Vec::new(),
            sensitivities: Vec::new(),
            purposes: Vec::new(),
            capabilities: Vec::new(),
            access_issuer,
        }
    }
}

#[derive(Debug, Clone)]
struct CredentialBinding {
    token_sha256: [u8; 32],
    principal_id: String,
    principal_kind: PrincipalKind,
    credential_id: String,
    grant_version: u64,
    workspaces: Vec<String>,
    namespaces: Vec<String>,
    agent_ids: Vec<String>,
    allow_shared_memory: bool,
    entity_keys: Vec<String>,
    data_subject_ids: Vec<String>,
    session_ids: Vec<String>,
    task_ids: Vec<String>,
    sensitivities: Vec<Sensitivity>,
    purposes: Vec<String>,
    capabilities: Vec<String>,
    active: bool,
    not_before_ms: Option<u64>,
    expires_at_ms: Option<u64>,
}

impl CredentialBinding {
    fn is_time_active(&self, now_ms: u64) -> bool {
        self.active
            && self
                .not_before_ms
                .map(|value| now_ms >= value)
                .unwrap_or(true)
            && self
                .expires_at_ms
                .map(|value| now_ms < value)
                .unwrap_or(true)
    }

    fn memory_context(
        &self,
        config: &AuthConfig,
        access_issuer: MemoryAccessIssuer,
    ) -> MemoryAuthContext {
        let process_workspace_id = memory_workspace(config);
        let eligible = self
            .workspaces
            .iter()
            .any(|workspace| workspace == &process_workspace_id);
        MemoryAuthContext {
            principal_id: self.principal_id.clone(),
            principal_kind: self.principal_kind,
            credential_id: self.credential_id.clone(),
            authenticated: true,
            eligible,
            denial_reason: "principal is not granted to this process Memory workspace",
            authorization_epoch: config.authorization_epoch,
            grant_version: self.grant_version,
            process_workspace_id,
            workspaces: self.workspaces.clone(),
            namespaces: self.namespaces.clone(),
            agent_ids: self.agent_ids.clone(),
            allow_shared_memory: self.allow_shared_memory,
            entity_keys: self.entity_keys.clone(),
            data_subject_ids: self.data_subject_ids.clone(),
            session_ids: self.session_ids.clone(),
            task_ids: self.task_ids.clone(),
            sensitivities: self.sensitivities.clone(),
            purposes: self.purposes.clone(),
            capabilities: self.capabilities.clone(),
            access_issuer,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Authentication<'a> {
    Principal(&'a CredentialBinding),
    Legacy,
    Unauthenticated,
    Disabled,
}

/// Resolved auth material for the running process.
#[derive(Debug, Clone)]
pub struct AuthRuntime {
    pub config: AuthConfig,
    /// Legacy global data-plane token, retained for compatibility and
    /// generation-control credential separation checks.
    pub token: Option<String>,
    pub bind_is_loopback: bool,
    principal_bindings: Vec<CredentialBinding>,
    memory_access_issuer: MemoryAccessIssuer,
}

impl AuthRuntime {
    /// Load or create a token according to config and bind address policy.
    pub fn bootstrap(config: AuthConfig, bind_host: &str) -> Result<Self, String> {
        Self::bootstrap_with_source(config, bind_host, "AKIDB_AUTH_TOKEN", "AkiDB auth token")
    }

    /// Bootstrap the always-authenticated generation publication boundary
    /// from a credential that is separate from the read data plane.
    pub fn bootstrap_generation_control(
        mut config: AuthConfig,
        bind_host: &str,
    ) -> Result<Self, String> {
        config.mode = AuthMode::Required;
        Self::bootstrap_with_source(
            config,
            bind_host,
            "AKIDB_GENERATION_CONTROL_TOKEN",
            "AkiDB generation-control token",
        )
    }

    fn bootstrap_with_source(
        config: AuthConfig,
        bind_host: &str,
        token_env: &str,
        token_label: &str,
    ) -> Result<Self, String> {
        validate_auth_config(&config)?;
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
            Some(resolve_or_create_token(&config, token_env, token_label)?)
        };
        let principal_bindings = resolve_principal_bindings(&config.principals)?;
        reject_duplicate_token_bindings(token.as_deref(), &principal_bindings)?;

        if required && token.as_ref().map(|value| value.is_empty()).unwrap_or(true) {
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
        if !bind_is_loopback && config.memory.allow_unauthenticated_loopback {
            return Err(
                "auth.memory.allow_unauthenticated_loopback cannot be used on a non-loopback bind"
                    .to_string(),
            );
        }

        Ok(Self {
            config,
            token,
            bind_is_loopback,
            principal_bindings,
            memory_access_issuer: MemoryAccessIssuer::new(),
        })
    }

    pub fn token_required(&self) -> bool {
        match self.config.mode {
            AuthMode::Disabled => false,
            AuthMode::Required => true,
            AuthMode::LoopbackOptional => !self.bind_is_loopback,
        }
    }

    /// Validate incoming metadata and build the legacy [`AuthContext`].
    pub fn authorize(&self, metadata: &MetadataMap) -> Result<AuthContext, Status> {
        self.authorize_all(metadata).map(|(legacy, _)| legacy)
    }

    /// Validate incoming metadata and derive the maximum Memory grants.
    pub fn authorize_memory(&self, metadata: &MetadataMap) -> Result<MemoryAuthContext, Status> {
        self.authorize_all(metadata).map(|(_, memory)| memory)
    }

    /// Verification half installed in the canonical Memory ledger owned by
    /// this authenticated server runtime.
    pub fn memory_access_verifier(&self) -> MemoryAccessVerifier {
        self.memory_access_issuer.verifier()
    }

    /// Process-internal proof for projection recovery, capability reporting,
    /// and canonical sequence barriers. It is never derived from or returned
    /// to a remote caller.
    pub fn memory_system_access_proof(&self) -> Result<MemoryAccessProof, String> {
        self.memory_access_issuer
            .issue(MemoryAccessGrant {
                principal_id: "system:memory-runtime".to_string(),
                credential_id: "internal:process".to_string(),
                workspace_id: memory_workspace(&self.config),
                namespace: "**".to_string(),
                request_purpose: "memory-maintenance".to_string(),
                delegated_agent_id: None,
                allow_shared_memory: true,
                entity_keys: vec!["**".to_string()],
                data_subject_ids: vec!["**".to_string()],
                require_data_subject: false,
                session_ids: vec!["**".to_string()],
                require_session: false,
                task_ids: vec!["**".to_string()],
                require_task: false,
                sensitivities: all_sensitivities(),
                capability: "memory.admin".to_string(),
                authorization_epoch: self.config.authorization_epoch,
                grant_version: 1,
                authorization_decision_id: format!("authz_{}", Uuid::now_v7().simple()),
                system_job: true,
            })
            .map_err(|error| format!("failed to issue internal Memory access proof: {error}"))
    }

    fn authorize_all(
        &self,
        metadata: &MetadataMap,
    ) -> Result<(AuthContext, MemoryAuthContext), Status> {
        let authentication = self.authenticate(metadata)?;
        let workspace_selector = metadata_selector(metadata, WORKSPACE_HEADER);
        let agent_selector = metadata_selector(metadata, AGENT_HEADER);

        let (workspace_id, agent_id, authenticated) = match authentication {
            Authentication::Principal(binding) => {
                let workspace_id = workspace_selector
                    .or_else(|| {
                        if binding
                            .workspaces
                            .iter()
                            .any(|workspace| workspace == &self.config.acl.default_workspace)
                        {
                            Some(self.config.acl.default_workspace.clone())
                        } else {
                            binding.workspaces.first().cloned()
                        }
                    })
                    .ok_or_else(|| {
                        Status::permission_denied("principal has no granted workspace")
                    })?;
                if !binding
                    .workspaces
                    .iter()
                    .any(|workspace| workspace == &workspace_id)
                {
                    return Err(Status::permission_denied(
                        "workspace selector is outside the principal grant",
                    ));
                }
                if let Some(agent_id) = &agent_selector {
                    if !binding
                        .agent_ids
                        .iter()
                        .any(|granted| granted == "**" || granted == agent_id)
                    {
                        return Err(Status::permission_denied(
                            "agent selector is outside the principal delegation grant",
                        ));
                    }
                }
                (workspace_id, agent_selector, true)
            }
            Authentication::Legacy => (
                workspace_selector.unwrap_or_else(|| self.config.acl.default_workspace.clone()),
                agent_selector,
                true,
            ),
            Authentication::Unauthenticated => (
                workspace_selector.unwrap_or_else(|| self.config.acl.default_workspace.clone()),
                agent_selector,
                false,
            ),
            Authentication::Disabled => (
                workspace_selector.unwrap_or_else(|| self.config.acl.default_workspace.clone()),
                agent_selector,
                true,
            ),
        };

        let memory = match authentication {
            Authentication::Principal(binding) => {
                binding.memory_context(&self.config, self.memory_access_issuer.clone())
            }
            Authentication::Legacy if self.config.memory.allow_legacy_principal => {
                legacy_memory_context(
                    &self.config,
                    self.memory_access_issuer.clone(),
                    "legacy-service",
                    "legacy-global-token",
                    true,
                )
            }
            Authentication::Legacy => MemoryAuthContext::denied(
                "legacy-service",
                "legacy-global-token",
                true,
                &self.config,
                self.memory_access_issuer.clone(),
                "the legacy global token has no authoritative Memory capabilities",
            ),
            Authentication::Unauthenticated
                if self.bind_is_loopback && self.config.memory.allow_unauthenticated_loopback =>
            {
                legacy_memory_context(
                    &self.config,
                    self.memory_access_issuer.clone(),
                    "insecure-local",
                    "none",
                    false,
                )
            }
            Authentication::Disabled if self.config.memory.allow_unauthenticated_loopback => {
                legacy_memory_context(
                    &self.config,
                    self.memory_access_issuer.clone(),
                    "insecure-development",
                    "none",
                    false,
                )
            }
            Authentication::Unauthenticated | Authentication::Disabled => {
                MemoryAuthContext::denied(
                    "unauthenticated-local",
                    "none",
                    false,
                    &self.config,
                    self.memory_access_issuer.clone(),
                    "authoritative Memory requires a principal credential",
                )
            }
        };

        Ok((
            AuthContext {
                workspace_id,
                agent_id,
                authenticated,
            },
            memory,
        ))
    }

    fn authenticate<'a>(&'a self, metadata: &MetadataMap) -> Result<Authentication<'a>, Status> {
        if self.config.mode == AuthMode::Disabled {
            return Ok(Authentication::Disabled);
        }

        let presented = extract_bearer(metadata);
        let Some(presented) = presented else {
            if self.token_required() {
                return Err(Status::unauthenticated(
                    "missing authorization bearer token",
                ));
            }
            return Ok(Authentication::Unauthenticated);
        };
        let presented_digest = token_sha256(presented.as_bytes());
        let now_ms = unix_time_ms().map_err(Status::internal)?;

        let mut matched_inactive_principal = false;
        for binding in &self.principal_bindings {
            if constant_time_eq(&binding.token_sha256, &presented_digest) {
                if binding.is_time_active(now_ms) {
                    return Ok(Authentication::Principal(binding));
                }
                matched_inactive_principal = true;
            }
        }
        if matched_inactive_principal {
            return Err(Status::unauthenticated(
                "principal credential is inactive or outside its validity window",
            ));
        }

        if let Some(expected) = &self.token {
            if constant_time_eq(expected.as_bytes(), presented.as_bytes()) {
                return Ok(Authentication::Legacy);
            }
        }
        Err(Status::unauthenticated("invalid bearer token"))
    }
}

/// Tonic interceptor that enforces bearer authentication and injects both the
/// legacy and strict Memory contexts.
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
        let (legacy, memory) = self.runtime.authorize_all(request.metadata())?;
        request.extensions_mut().insert(legacy);
        request.extensions_mut().insert(memory);
        Ok(request)
    }
}

/// Extract legacy [`AuthContext`] from a typed request (after interceptor).
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

/// Extract the principal-derived Memory context. Missing interceptor state
/// fails closed.
pub fn memory_auth_context<T>(request: &Request<T>) -> Result<MemoryAuthContext, Status> {
    request
        .extensions()
        .get::<MemoryAuthContext>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Memory authentication context is missing"))
}

fn legacy_memory_context(
    config: &AuthConfig,
    access_issuer: MemoryAccessIssuer,
    principal_id: &str,
    credential_id: &str,
    authenticated: bool,
) -> MemoryAuthContext {
    let workspace = memory_workspace(config);
    MemoryAuthContext {
        principal_id: principal_id.to_string(),
        principal_kind: PrincipalKind::Service,
        credential_id: credential_id.to_string(),
        authenticated,
        eligible: true,
        denial_reason: "",
        authorization_epoch: config.authorization_epoch,
        grant_version: 1,
        process_workspace_id: workspace.clone(),
        workspaces: vec![workspace],
        namespaces: vec!["**".to_string()],
        agent_ids: vec!["**".to_string()],
        allow_shared_memory: true,
        entity_keys: vec!["**".to_string()],
        data_subject_ids: vec!["**".to_string()],
        session_ids: vec!["**".to_string()],
        task_ids: vec!["**".to_string()],
        sensitivities: all_sensitivities(),
        purposes: vec!["**".to_string()],
        capabilities: MEMORY_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        access_issuer,
    }
}

fn memory_workspace(config: &AuthConfig) -> String {
    let configured = config.memory.workspace_id.trim();
    if configured.is_empty() {
        config.acl.default_workspace.clone()
    } else {
        configured.to_string()
    }
}

fn namespace_matches(grant: &str, namespace: &str) -> bool {
    if grant == "**" {
        return true;
    }
    if let Some(prefix) = grant.strip_suffix("/**") {
        return namespace == prefix
            || namespace
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'));
    }
    grant == namespace
}

fn effective_scope_values(
    field: &str,
    grants: &[String],
    requested: &[String],
) -> Result<Vec<String>, Status> {
    if requested.is_empty() {
        return Ok(grants.to_vec());
    }
    let mut unique = HashSet::with_capacity(requested.len());
    for value in requested {
        validate_request_selector(field, value)?;
        if value.contains('*') {
            return Err(Status::invalid_argument(format!(
                "{field} request selectors cannot contain wildcards"
            )));
        }
        if !unique.insert(value) {
            return Err(Status::invalid_argument(format!(
                "{field} request selectors cannot contain duplicates"
            )));
        }
        if !grants.iter().any(|grant| grant == "**" || grant == value) {
            return Err(Status::permission_denied(format!(
                "requested Memory {field} selector is outside the principal grant"
            )));
        }
    }
    Ok(requested.to_vec())
}

fn effective_sensitivities(
    grants: &[Sensitivity],
    maximum: Option<Sensitivity>,
) -> Result<Vec<Sensitivity>, Status> {
    let Some(maximum) = maximum else {
        return Ok(grants.to_vec());
    };
    let values = grants
        .iter()
        .copied()
        .filter(|value| sensitivity_rank(*value) <= sensitivity_rank(maximum))
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(Status::permission_denied(
            "requested maximum Memory sensitivity excludes every principal grant",
        ));
    }
    Ok(values)
}

fn parse_sensitivity_grants(values: &[String]) -> Result<Vec<Sensitivity>, String> {
    if values.is_empty() {
        return Err("must declare at least one Memory sensitivity grant".to_string());
    }
    values
        .iter()
        .map(|value| match value.as_str() {
            "public" => Ok(Sensitivity::Public),
            "internal" => Ok(Sensitivity::Internal),
            "confidential" => Ok(Sensitivity::Confidential),
            "restricted" => Ok(Sensitivity::Restricted),
            other => Err(format!("has unknown Memory sensitivity {other}")),
        })
        .collect()
}

fn all_sensitivities() -> Vec<Sensitivity> {
    vec![
        Sensitivity::Public,
        Sensitivity::Internal,
        Sensitivity::Confidential,
        Sensitivity::Restricted,
    ]
}

fn sensitivity_rank(value: Sensitivity) -> u8 {
    match value {
        Sensitivity::Public => 1,
        Sensitivity::Internal => 2,
        Sensitivity::Confidential => 3,
        Sensitivity::Restricted => 4,
    }
}

fn metadata_selector(metadata: &MetadataMap, name: &'static str) -> Option<String> {
    metadata
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn validate_request_selector(field: &str, value: &str) -> Result<(), Status> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_AUTH_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Status::invalid_argument(format!(
            "{field} must be non-empty, trimmed, bounded, and contain no control characters"
        )));
    }
    Ok(())
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

fn validate_auth_config(config: &AuthConfig) -> Result<(), String> {
    if config.authorization_epoch == 0 {
        return Err("auth.authorization_epoch must be greater than zero".to_string());
    }
    validate_config_value("auth.acl.default_workspace", &config.acl.default_workspace)?;
    if !config.memory.workspace_id.trim().is_empty() {
        validate_config_value("auth.memory.workspace_id", &config.memory.workspace_id)?;
    }
    Ok(())
}

fn resolve_principal_bindings(
    principals: &[PrincipalConfig],
) -> Result<Vec<CredentialBinding>, String> {
    let mut principal_ids = HashSet::new();
    let mut credential_ids = HashSet::new();
    let mut token_digests: Vec<[u8; 32]> = Vec::new();
    let mut bindings = Vec::new();

    for principal in principals {
        validate_config_value("principal_id", &principal.principal_id)?;
        if !principal_ids.insert(principal.principal_id.clone()) {
            return Err(format!(
                "duplicate auth principal_id {}",
                principal.principal_id
            ));
        }
        if principal.grant_version == 0 {
            return Err(format!(
                "principal {} grant_version must be greater than zero",
                principal.principal_id
            ));
        }
        validate_unique_config_values(
            &format!("principal {} workspaces", principal.principal_id),
            &principal.workspaces,
        )?;
        validate_unique_config_values(
            &format!("principal {} namespaces", principal.principal_id),
            &principal.namespaces,
        )?;
        validate_unique_config_values(
            &format!("principal {} agent_ids", principal.principal_id),
            &principal.agent_ids,
        )?;
        validate_unique_config_values(
            &format!("principal {} entity_keys", principal.principal_id),
            &principal.entity_keys,
        )?;
        validate_unique_config_values(
            &format!("principal {} data_subject_ids", principal.principal_id),
            &principal.data_subject_ids,
        )?;
        validate_unique_config_values(
            &format!("principal {} session_ids", principal.principal_id),
            &principal.session_ids,
        )?;
        validate_unique_config_values(
            &format!("principal {} task_ids", principal.principal_id),
            &principal.task_ids,
        )?;
        validate_unique_config_values(
            &format!("principal {} sensitivities", principal.principal_id),
            &principal.sensitivities,
        )?;
        if principal.entity_keys.is_empty() {
            return Err(format!(
                "principal {} must declare at least one Memory entity_key grant",
                principal.principal_id
            ));
        }
        let sensitivities = parse_sensitivity_grants(&principal.sensitivities)
            .map_err(|error| format!("principal {} {error}", principal.principal_id))?;
        validate_unique_config_values(
            &format!("principal {} purposes", principal.principal_id),
            &principal.purposes,
        )?;
        validate_unique_config_values(
            &format!("principal {} capabilities", principal.principal_id),
            &principal.capabilities,
        )?;
        for capability in &principal.capabilities {
            if !MEMORY_CAPABILITIES.contains(&capability.as_str()) {
                return Err(format!(
                    "principal {} has unknown Memory capability {}",
                    principal.principal_id, capability
                ));
            }
        }
        if principal.credentials.is_empty() {
            return Err(format!(
                "principal {} must declare at least one credential",
                principal.principal_id
            ));
        }

        for credential in &principal.credentials {
            validate_config_value("credential_id", &credential.credential_id)?;
            if !credential_ids.insert(credential.credential_id.clone()) {
                return Err(format!(
                    "duplicate auth credential_id {}",
                    credential.credential_id
                ));
            }
            if let (Some(not_before), Some(expires_at)) =
                (credential.not_before_ms, credential.expires_at_ms)
            {
                if expires_at <= not_before {
                    return Err(format!(
                        "credential {} expires_at_ms must be greater than not_before_ms",
                        credential.credential_id
                    ));
                }
            }
            let token = resolve_principal_token(credential)?;
            if token.len() < MIN_PRINCIPAL_TOKEN_BYTES {
                return Err(format!(
                    "credential {} token must be at least {MIN_PRINCIPAL_TOKEN_BYTES} bytes",
                    credential.credential_id
                ));
            }
            let digest = token_sha256(token.as_bytes());
            if token_digests
                .iter()
                .any(|existing| constant_time_eq(existing, &digest))
            {
                return Err("the same bearer token is bound to multiple credentials".to_string());
            }
            token_digests.push(digest);
            bindings.push(CredentialBinding {
                token_sha256: digest,
                principal_id: principal.principal_id.clone(),
                principal_kind: principal.kind,
                credential_id: credential.credential_id.clone(),
                grant_version: principal.grant_version,
                workspaces: principal.workspaces.clone(),
                namespaces: principal.namespaces.clone(),
                agent_ids: principal.agent_ids.clone(),
                allow_shared_memory: principal.allow_shared_memory,
                entity_keys: principal.entity_keys.clone(),
                data_subject_ids: principal.data_subject_ids.clone(),
                session_ids: principal.session_ids.clone(),
                task_ids: principal.task_ids.clone(),
                sensitivities: sensitivities.clone(),
                purposes: principal.purposes.clone(),
                capabilities: principal.capabilities.clone(),
                active: principal.active && credential.active,
                not_before_ms: credential.not_before_ms,
                expires_at_ms: credential.expires_at_ms,
            });
        }
    }
    Ok(bindings)
}

fn resolve_principal_token(config: &PrincipalCredentialConfig) -> Result<String, String> {
    let inline = config
        .token
        .as_ref()
        .filter(|value| !value.is_empty())
        .map(|value| ("token", value.clone()));
    let file = config
        .token_file
        .as_ref()
        .filter(|value| !value.is_empty())
        .map(|value| ("token_file", value.clone()));
    let env = config
        .token_env
        .as_ref()
        .filter(|value| !value.is_empty())
        .map(|value| ("token_env", value.clone()));
    let sources: Vec<_> = [inline, file, env].into_iter().flatten().collect();
    if sources.len() != 1 {
        return Err(format!(
            "credential {} must configure exactly one of token, token_file, or token_env",
            config.credential_id
        ));
    }
    match &sources[0] {
        ("token", token) => validate_loaded_token(&config.credential_id, token),
        ("token_file", path) => {
            let path = PathBuf::from(path);
            verify_private_token_file(&path)?;
            let token = fs::read_to_string(&path).map_err(|error| {
                format!(
                    "failed to read credential {} token file {}: {error}",
                    config.credential_id,
                    path.display()
                )
            })?;
            validate_loaded_token(&config.credential_id, token.trim())
        }
        ("token_env", name) => {
            validate_config_value("credential token_env", name)?;
            let token = std::env::var(name).map_err(|_| {
                format!(
                    "credential {} token environment variable {} is missing",
                    config.credential_id, name
                )
            })?;
            validate_loaded_token(&config.credential_id, token.trim())
        }
        _ => unreachable!("credential source was validated"),
    }
}

fn validate_loaded_token(credential_id: &str, token: &str) -> Result<String, String> {
    if token.is_empty() || token.trim() != token || token.chars().any(char::is_control) {
        return Err(format!(
            "credential {credential_id} token must be non-empty, trimmed, and contain no controls"
        ));
    }
    Ok(token.to_string())
}

fn reject_duplicate_token_bindings(
    legacy_token: Option<&str>,
    bindings: &[CredentialBinding],
) -> Result<(), String> {
    let Some(legacy_token) = legacy_token else {
        return Ok(());
    };
    let digest = token_sha256(legacy_token.as_bytes());
    if bindings
        .iter()
        .any(|binding| constant_time_eq(&binding.token_sha256, &digest))
    {
        return Err(
            "the legacy global bearer token must not also identify a configured principal"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_unique_config_values(label: &str, values: &[String]) -> Result<(), String> {
    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        validate_config_value(label, value)?;
        if !unique.insert(value) {
            return Err(format!("{label} contains duplicate value {value}"));
        }
    }
    Ok(())
}

fn validate_config_value(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_AUTH_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(format!(
            "{field} must be non-empty, trimmed, at most {MAX_AUTH_VALUE_BYTES} bytes, and contain no controls"
        ));
    }
    Ok(())
}

fn resolve_or_create_token(
    config: &AuthConfig,
    token_env: &str,
    token_label: &str,
) -> Result<String, String> {
    if let Some(token) = config.token.as_ref().map(|value| value.trim().to_string()) {
        if !token.is_empty() {
            return Ok(token);
        }
    }
    if let Ok(env) = std::env::var(token_env) {
        let env = env.trim().to_string();
        if !env.is_empty() {
            return Ok(env);
        }
    }

    let path = PathBuf::from(&config.token_file);
    if path.exists() {
        verify_private_token_file(&path)?;
        let token = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read auth token file {}: {error}", path.display()))?
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
        env = token_env,
        "generated new auth token file (store securely)"
    );
    eprintln!("{token_label} (save this): {token}");
    Ok(token)
}

fn verify_private_token_file(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect token file {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing symlink token file {}; use a regular mode-0600 file",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(format!(
            "token path {} is not a regular file",
            path.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "token file {} permissions are {:o}; expected no group/other access",
                path.display(),
                mode & 0o777
            ));
        }
    }
    Ok(())
}

fn write_token_file(path: &Path, token: &str) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create token directory {}: {error}",
                parent.display()
            )
        })?;
    }
    fs::write(path, format!("{token}\n"))
        .map_err(|error| format!("failed to write token file {}: {error}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| {
            format!(
                "failed to set token file {} mode 0600: {error}",
                path.display()
            )
        })?;
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

fn unix_time_ms() -> Result<u64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes Unix epoch: {error}"))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| "system time milliseconds overflow u64".to_string())
}

fn token_sha256(token: &[u8]) -> [u8; 32] {
    Sha256::digest(token).into()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_common::config::{AclConfig, MemoryAuthorizationConfig, PrincipalCredentialConfig};
    use tonic::metadata::MetadataValue;

    fn runtime(mode: AuthMode, token: Option<&str>, loopback: bool) -> AuthRuntime {
        let config = AuthConfig {
            mode,
            token_file: "./data/auth.token".into(),
            token: token.map(str::to_string),
            acl: AclConfig::default(),
            ..AuthConfig::default()
        };
        AuthRuntime {
            config,
            token: token.map(str::to_string),
            bind_is_loopback: loopback,
            principal_bindings: Vec::new(),
            memory_access_issuer: MemoryAccessIssuer::new(),
        }
    }

    fn principal(token: &str) -> PrincipalConfig {
        PrincipalConfig {
            principal_id: "service:coding-agent".to_string(),
            kind: PrincipalKind::Service,
            active: true,
            grant_version: 7,
            credentials: vec![PrincipalCredentialConfig {
                credential_id: "coding-agent-2026-07".to_string(),
                token: Some(token.to_string()),
                token_file: None,
                token_env: None,
                active: true,
                not_before_ms: None,
                expires_at_ms: None,
            }],
            workspaces: vec!["workspace-a".to_string()],
            namespaces: vec!["repo/**".to_string()],
            agent_ids: vec!["agent:codex".to_string()],
            allow_shared_memory: false,
            entity_keys: vec!["service:ingestion".to_string()],
            data_subject_ids: vec!["**".to_string()],
            session_ids: vec!["session-1".to_string()],
            task_ids: vec!["task-1".to_string()],
            sensitivities: vec![
                "public".to_string(),
                "internal".to_string(),
                "confidential".to_string(),
                "restricted".to_string(),
            ],
            purposes: vec!["debugging".to_string()],
            capabilities: vec![
                "memory.remember".to_string(),
                "memory.read".to_string(),
                "memory.recall".to_string(),
            ],
        }
    }

    fn principal_runtime() -> AuthRuntime {
        let config = AuthConfig {
            mode: AuthMode::Required,
            token_file: "./unused-principal-test.token".to_string(),
            token: Some("legacy-token-separate".to_string()),
            acl: AclConfig {
                default_workspace: "workspace-a".to_string(),
                enforce_workspace: true,
            },
            principals: vec![principal("principal-secret-0001")],
            authorization_epoch: 11,
            memory: MemoryAuthorizationConfig {
                workspace_id: "workspace-a".to_string(),
                allow_legacy_principal: false,
                allow_unauthenticated_loopback: false,
            },
        };
        AuthRuntime::bootstrap(config, "127.0.0.1").unwrap()
    }

    fn bearer(value: &'static str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(AUTH_HEADER, MetadataValue::from_static(value));
        metadata
    }

    #[test]
    fn loopback_optional_allows_missing_token_on_loopback() {
        let runtime = runtime(AuthMode::LoopbackOptional, Some("secret"), true);
        let context = runtime.authorize(&MetadataMap::new()).unwrap();
        assert!(!context.authenticated);
        assert_eq!(context.workspace_id, "default");
    }

    #[test]
    fn loopback_optional_requires_token_off_loopback() {
        let runtime = runtime(AuthMode::LoopbackOptional, Some("secret"), false);
        let error = runtime.authorize(&MetadataMap::new()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn legacy_data_plane_keeps_workspace_header_compatibility() {
        let runtime = runtime(AuthMode::Required, Some("secret"), true);
        let mut metadata = bearer("Bearer secret");
        metadata.insert(WORKSPACE_HEADER, MetadataValue::from_static("team-a"));
        let context = runtime.authorize(&metadata).unwrap();
        assert!(context.authenticated);
        assert_eq!(context.workspace_id, "team-a");
    }

    #[test]
    fn rejects_invalid_token() {
        let runtime = runtime(AuthMode::Required, Some("secret"), true);
        assert!(runtime.authorize(&bearer("Bearer wrong")).is_err());
    }

    #[test]
    fn principal_request_scope_can_only_narrow_grants() {
        let runtime = principal_runtime();
        let metadata = bearer("Bearer principal-secret-0001");
        let memory = runtime.authorize_memory(&metadata).unwrap();

        let authorized = memory
            .authorize_scope(
                "workspace-a",
                "repo/akidb",
                "debugging",
                Some("agent:codex"),
                "memory.remember",
            )
            .unwrap();
        assert_eq!(authorized.principal_id(), "service:coding-agent");
        assert_eq!(authorized.workspace_id(), "workspace-a");
        assert_eq!(authorized.authorization_epoch(), 11);
        assert_eq!(authorized.grant_version(), 7);
        runtime
            .memory_access_verifier()
            .verify(authorized.storage_proof())
            .unwrap();
        assert_eq!(
            authorized.storage_proof().authorization_decision_id(),
            authorized.authorization_decision_id()
        );
        assert_eq!(authorized.storage_proof().capability(), "memory.remember");
        assert!(MemoryAccessIssuer::new()
            .verifier()
            .verify(authorized.storage_proof())
            .is_err());

        let narrowed = memory
            .authorize_scoped(
                "workspace-a",
                "repo/akidb",
                "debugging",
                Some("agent:codex"),
                &MemoryScopeSelector {
                    entity_keys: vec!["service:ingestion".to_string()],
                    data_subject_ids: vec!["subject-42".to_string()],
                    session_ids: vec!["session-1".to_string()],
                    task_ids: vec!["task-1".to_string()],
                    maximum_sensitivity: Some(Sensitivity::Internal),
                },
                "memory.remember",
            )
            .unwrap();
        runtime
            .memory_access_verifier()
            .verify(narrowed.storage_proof())
            .unwrap();
        assert_ne!(
            authorized.storage_proof().scope_sha256(),
            narrowed.storage_proof().scope_sha256()
        );
        for selector in [
            MemoryScopeSelector {
                entity_keys: vec!["service:indexer".to_string()],
                ..MemoryScopeSelector::default()
            },
            MemoryScopeSelector {
                session_ids: vec!["session-2".to_string()],
                ..MemoryScopeSelector::default()
            },
            MemoryScopeSelector {
                task_ids: vec!["task-2".to_string()],
                ..MemoryScopeSelector::default()
            },
        ] {
            assert_eq!(
                memory
                    .authorize_scoped(
                        "workspace-a",
                        "repo/akidb",
                        "debugging",
                        Some("agent:codex"),
                        &selector,
                        "memory.remember",
                    )
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        assert_eq!(
            memory
                .authorize_scoped(
                    "workspace-a",
                    "repo/akidb",
                    "debugging",
                    Some("agent:codex"),
                    &MemoryScopeSelector {
                        entity_keys: vec!["service:*".to_string()],
                        ..MemoryScopeSelector::default()
                    },
                    "memory.remember",
                )
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        assert_eq!(
            memory
                .authorize_scope(
                    "workspace-b",
                    "repo/akidb",
                    "debugging",
                    Some("agent:codex"),
                    "memory.remember",
                )
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert!(memory
            .authorize_scope(
                "workspace-a",
                "private/payroll",
                "debugging",
                Some("agent:codex"),
                "memory.remember",
            )
            .is_err());
        assert!(memory
            .authorize_scope(
                "workspace-a",
                "repo/akidb",
                "marketing",
                Some("agent:codex"),
                "memory.remember",
            )
            .is_err());
        assert!(memory
            .authorize_scope(
                "workspace-a",
                "repo/akidb",
                "debugging",
                Some("agent:other"),
                "memory.remember",
            )
            .is_err());
        assert!(memory
            .authorize_scope(
                "workspace-a",
                "repo/akidb",
                "debugging",
                Some("agent:codex"),
                "memory.export",
            )
            .is_err());
    }

    #[test]
    fn principal_workspace_header_cannot_expand_data_plane_scope() {
        let runtime = principal_runtime();
        let mut metadata = bearer("Bearer principal-secret-0001");
        metadata.insert(WORKSPACE_HEADER, MetadataValue::from_static("workspace-b"));
        assert_eq!(
            runtime.authorize(&metadata).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn principal_agent_wildcard_allows_an_exact_request_selector() {
        let mut config = principal_runtime().config;
        config.principals[0].agent_ids = vec!["**".to_string()];
        let runtime = AuthRuntime::bootstrap(config, "127.0.0.1").unwrap();
        let mut metadata = bearer("Bearer principal-secret-0001");
        metadata.insert(AGENT_HEADER, MetadataValue::from_static("agent:other"));
        let (legacy, memory) = runtime.authorize_all(&metadata).unwrap();
        assert_eq!(legacy.agent_id.as_deref(), Some("agent:other"));
        memory
            .authorize_scope(
                "workspace-a",
                "repo/akidb",
                "debugging",
                Some("agent:other"),
                "memory.remember",
            )
            .unwrap();
    }

    #[test]
    fn legacy_token_has_no_memory_capabilities_by_default() {
        let runtime = principal_runtime();
        let memory = runtime
            .authorize_memory(&bearer("Bearer legacy-token-separate"))
            .unwrap();
        assert!(memory.authenticated());
        assert_eq!(
            memory
                .authorize_scope(
                    "workspace-a",
                    "repo/akidb",
                    "debugging",
                    None,
                    "memory.read",
                )
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn inactive_or_expired_principal_credential_is_rejected() {
        let mut config = principal_runtime().config;
        config.principals[0].credentials[0].expires_at_ms = Some(1);
        let runtime = AuthRuntime::bootstrap(config, "127.0.0.1").unwrap();
        assert_eq!(
            runtime
                .authorize(&bearer("Bearer principal-secret-0001"))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn rotation_accepts_two_distinct_active_credentials() {
        let mut config = principal_runtime().config;
        config.principals[0]
            .credentials
            .push(PrincipalCredentialConfig {
                credential_id: "coding-agent-2026-08".to_string(),
                token: Some("principal-secret-0002".to_string()),
                token_file: None,
                token_env: None,
                active: true,
                not_before_ms: None,
                expires_at_ms: None,
            });
        let runtime = AuthRuntime::bootstrap(config, "127.0.0.1").unwrap();
        assert_eq!(
            runtime
                .authorize_memory(&bearer("Bearer principal-secret-0001"))
                .unwrap()
                .credential_id(),
            "coding-agent-2026-07"
        );
        assert_eq!(
            runtime
                .authorize_memory(&bearer("Bearer principal-secret-0002"))
                .unwrap()
                .credential_id(),
            "coding-agent-2026-08"
        );
    }

    #[test]
    fn duplicate_token_binding_is_rejected() {
        let mut config = principal_runtime().config;
        let mut duplicate = principal("principal-secret-0001");
        duplicate.principal_id = "service:other".to_string();
        duplicate.credentials[0].credential_id = "other-credential".to_string();
        config.principals.push(duplicate);
        assert!(AuthRuntime::bootstrap(config, "127.0.0.1").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn principal_token_file_rejects_group_or_world_access() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let token_path = directory.path().join("principal.token");
        fs::write(&token_path, "principal-secret-0001\n").unwrap();
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o644)).unwrap();

        let mut config = principal_runtime().config;
        config.principals[0].credentials[0].token = None;
        config.principals[0].credentials[0].token_file =
            Some(token_path.to_string_lossy().to_string());
        assert!(AuthRuntime::bootstrap(config, "127.0.0.1").is_err());
    }

    #[test]
    fn generation_control_bootstrap_is_always_authenticated() {
        let config = AuthConfig {
            mode: AuthMode::Disabled,
            token_file: "./unused-generation-control.token".into(),
            token: Some("control-secret".to_string()),
            acl: AclConfig::default(),
            ..AuthConfig::default()
        };
        let runtime = AuthRuntime::bootstrap_generation_control(config, "127.0.0.1").unwrap();
        assert!(runtime.token_required());
        assert_eq!(
            runtime.authorize(&MetadataMap::new()).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn detects_loopback_hosts() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("192.168.1.10"));
    }

    #[test]
    fn namespace_prefix_grant_does_not_match_sibling_prefix() {
        assert!(namespace_matches("repo/**", "repo/akidb"));
        assert!(namespace_matches("repo/**", "repo"));
        assert!(!namespace_matches("repo/**", "repository/private"));
    }
}

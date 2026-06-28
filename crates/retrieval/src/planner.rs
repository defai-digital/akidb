//! Deterministic retrieval planner.
//!
//! This is the first planner layer for AkiDB's AI-native retrieval path. It is
//! intentionally heuristic and dependency-free: callers can inspect the trace,
//! override the mode, and later replace or augment this with a model-based
//! planner without changing the retrieval stages.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrievalMode {
    Auto,
    Vector,
    Bm25,
    Hybrid,
    Graph,
    GraphHybrid,
    StructuredSql,
}

#[derive(Debug, Clone)]
pub struct PlannerInput {
    pub query_text: String,
    pub requested_mode: Option<RetrievalMode>,
    pub has_metadata_filter: bool,
    pub pack: bool,
}

impl PlannerInput {
    pub fn new(query_text: impl Into<String>) -> Self {
        Self {
            query_text: query_text.into(),
            requested_mode: None,
            has_metadata_filter: false,
            pack: false,
        }
    }

    pub fn with_requested_mode(mut self, mode: RetrievalMode) -> Self {
        self.requested_mode = Some(mode);
        self
    }

    pub fn with_metadata_filter(mut self, has_filter: bool) -> Self {
        self.has_metadata_filter = has_filter;
        self
    }

    pub fn with_pack(mut self, pack: bool) -> Self {
        self.pack = pack;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannerTrace {
    pub mode: RetrievalMode,
    pub vector_weight: f32,
    pub lexical_weight: f32,
    pub graph_enabled: bool,
    pub graph_depth: u8,
    pub reasons: Vec<String>,
}

pub fn plan_query(input: &PlannerInput) -> PlannerTrace {
    if let Some(mode) = input.requested_mode.filter(|m| *m != RetrievalMode::Auto) {
        return trace_for_mode(mode, vec!["explicit retrieval mode requested".to_string()]);
    }

    let q = input.query_text.trim();
    let lower = q.to_ascii_lowercase();
    let mut reasons = Vec::new();

    let has_identifier = q
        .split_whitespace()
        .any(|token| looks_like_identifier(token) || looks_like_path(token));
    let relationship = starts_with_any(
        &lower,
        &[
            "what calls",
            "who calls",
            "what depends",
            "what imports",
            "who changed",
            "what changed",
            "who modified",
            "what modified",
            "who updated",
            "what updated",
            "who edited",
            "what edited",
            "dependency",
            "dependencies",
        ],
    );
    let explanatory = starts_with_any(
        &lower,
        &[
            "explain",
            "how does",
            "how do",
            "why",
            "summarize",
            "describe",
        ],
    );
    let quoted = q.contains('"') || q.contains('\'');

    let mode = if relationship {
        reasons.push("relationship query detected".to_string());
        RetrievalMode::GraphHybrid
    } else if has_identifier || quoted {
        reasons.push("identifier/path/exact term signal detected".to_string());
        RetrievalMode::Hybrid
    } else if explanatory {
        reasons.push("explanatory query detected".to_string());
        RetrievalMode::Hybrid
    } else {
        reasons.push("default hybrid retrieval".to_string());
        RetrievalMode::Hybrid
    };

    if input.has_metadata_filter {
        reasons.push("metadata filter present".to_string());
    }
    if input.pack && matches!(mode, RetrievalMode::Hybrid | RetrievalMode::GraphHybrid) {
        reasons.push("context packing requested".to_string());
    }

    trace_for_mode(mode, reasons)
}

fn trace_for_mode(mode: RetrievalMode, reasons: Vec<String>) -> PlannerTrace {
    let (vector_weight, lexical_weight, graph_enabled, graph_depth) = match mode {
        RetrievalMode::Auto | RetrievalMode::Hybrid => (1.0, 1.0, false, 0),
        RetrievalMode::Vector => (1.0, 0.0, false, 0),
        RetrievalMode::Bm25 => (0.0, 1.0, false, 0),
        RetrievalMode::Graph => (0.0, 0.5, true, 2),
        RetrievalMode::GraphHybrid => (1.0, 1.0, true, 2),
        RetrievalMode::StructuredSql => (0.0, 0.0, false, 0),
    };
    PlannerTrace {
        mode,
        vector_weight,
        lexical_weight,
        graph_enabled,
        graph_depth,
        reasons,
    }
}

fn starts_with_any(s: &str, prefixes: &[&str]) -> bool {
    prefixes.iter().any(|prefix| s.starts_with(prefix))
}

fn looks_like_identifier(token: &str) -> bool {
    let t = token.trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != ':' && c != '.');
    t.contains("::")
        || t.contains('_')
        || t.chars().any(|c| c.is_ascii_uppercase())
        || t.ends_with("()")
}

fn looks_like_path(token: &str) -> bool {
    let t = token.trim_matches(|c: char| c == ',' || c == ';' || c == ':' || c == ')' || c == '(');
    t.contains('/')
        || t.ends_with(".rs")
        || t.ends_with(".py")
        || t.ends_with(".ts")
        || t.ends_with(".tsx")
        || t.ends_with(".js")
        || t.ends_with(".md")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_query_uses_hybrid() {
        let trace = plan_query(&PlannerInput::new("find MtpScheduler in mtp_scheduler.rs"));
        assert_eq!(trace.mode, RetrievalMode::Hybrid);
        assert!(trace.lexical_weight > 0.0);
    }

    #[test]
    fn test_relationship_query_enables_graph() {
        let trace = plan_query(&PlannerInput::new("what calls draft_model.rs"));
        assert_eq!(trace.mode, RetrievalMode::GraphHybrid);
        assert!(trace.graph_enabled);
        assert_eq!(trace.graph_depth, 2);
    }

    #[test]
    fn test_modified_query_enables_graph() {
        let trace = plan_query(&PlannerInput::new("who modified mtp_scheduler.rs"));
        assert_eq!(trace.mode, RetrievalMode::GraphHybrid);
        assert!(trace.graph_enabled);
    }

    #[test]
    fn test_explicit_mode_wins() {
        let trace = plan_query(
            &PlannerInput::new("explain scheduler").with_requested_mode(RetrievalMode::Vector),
        );
        assert_eq!(trace.mode, RetrievalMode::Vector);
        assert_eq!(trace.lexical_weight, 0.0);
    }
}

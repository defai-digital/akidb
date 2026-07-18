//! Side effects requested by components and executed by the async runtime.

use crate::model::ImportPlanInput;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    LoadCapabilities,
    LoadCollections,
    LoadOperations,
    LoadSnapshots,
    PlanImport(ImportPlanInput),
    LoadAudit,
}

impl Effect {
    /// Stable category used to deduplicate in-flight/pending work.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::LoadCapabilities => "capabilities.read",
            Self::LoadCollections => "collections.read",
            Self::LoadOperations => "operations.read",
            Self::LoadSnapshots => "snapshots.read",
            Self::PlanImport(_) => "data.import.plan",
            Self::LoadAudit => "audit.read",
        }
    }

    pub fn is_read_or_validate_only(&self) -> bool {
        matches!(
            self,
            Self::LoadCapabilities
                | Self::LoadCollections
                | Self::LoadOperations
                | Self::LoadSnapshots
                | Self::PlanImport(_)
                | Self::LoadAudit
        )
    }

    pub fn all_v1() -> Vec<Self> {
        vec![
            Self::LoadCapabilities,
            Self::LoadCollections,
            Self::LoadOperations,
            Self::LoadSnapshots,
            Self::PlanImport(ImportPlanInput::default()),
            Self::LoadAudit,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_effect_allowlist_contains_only_read_or_validation_work() {
        assert!(Effect::all_v1()
            .iter()
            .all(Effect::is_read_or_validate_only));
        assert!(Effect::all_v1().iter().all(|effect| {
            !effect.kind().contains("execute")
                && !effect.kind().contains("delete")
                && !effect.kind().contains("restore")
                && !effect.kind().contains("cancel")
        }));
    }
}

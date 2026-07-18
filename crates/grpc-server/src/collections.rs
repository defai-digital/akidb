//! In-process collection registry (GAP-005 / SCH-101).
//!
//! v3.1 tracks collection *schema* metadata for the active shard index. Multi-
//! index routing is out of scope; CreateCollection registers/validates schema
//! against the running index dimensions when the name matches the shard
//! collection, or records additional named schemas for future expansion.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub name: String,
    pub dimensions: u32,
    pub metric: String,
    pub embedding_model_id: String,
    pub vector_precision: String,
    pub chunk_strategy: String,
}

#[derive(Debug, Default)]
pub struct CollectionRegistry {
    collections: RwLock<HashMap<String, CollectionMeta>>,
}

impl CollectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure_default(
        &self,
        name: &str,
        dimensions: u32,
        metric: &str,
        precision: &str,
        embedding_model_id: &str,
    ) {
        let mut guard = self.collections.write();
        guard
            .entry(name.to_string())
            .or_insert_with(|| CollectionMeta {
                name: name.to_string(),
                dimensions,
                metric: metric.to_string(),
                embedding_model_id: embedding_model_id.to_string(),
                vector_precision: precision.to_string(),
                chunk_strategy: "fixed".to_string(),
            });
    }

    pub fn create(&self, meta: CollectionMeta) -> Result<CollectionMeta, String> {
        if meta.name.trim().is_empty() {
            return Err("collection name cannot be empty".to_string());
        }
        if meta.dimensions == 0 {
            return Err("dimensions must be > 0".to_string());
        }
        let mut guard = self.collections.write();
        if guard.contains_key(&meta.name) {
            return Err(format!("collection '{}' already exists", meta.name));
        }
        guard.insert(meta.name.clone(), meta.clone());
        Ok(meta)
    }

    pub fn get(&self, name: &str) -> Option<CollectionMeta> {
        self.collections.read().get(name).cloned()
    }

    pub fn list(&self) -> Vec<CollectionMeta> {
        let mut items: Vec<_> = self.collections.read().values().cloned().collect();
        items.sort_by(|a, b| a.name.cmp(&b.name));
        items
    }

    pub fn remove(&self, name: &str) -> Result<(), String> {
        let mut guard = self.collections.write();
        if guard.remove(name).is_some() {
            Ok(())
        } else {
            Err(format!("collection '{name}' not found"))
        }
    }
}

pub type SharedCollectionRegistry = Arc<CollectionRegistry>;

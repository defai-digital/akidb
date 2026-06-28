//! Optional SQL metadata index for structured RAG filters.
//!
//! This crate intentionally models metadata indexing, not a full SQL engine.
//! AkiDB keeps vector/BM25/graph retrieval in the core path and uses this layer
//! to answer exact structured filters over chunk metadata.

use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::sync::Mutex;
use thiserror::Error;

#[cfg(feature = "postgres")]
use postgres::types::ToSql;
#[cfg(feature = "postgres")]
use postgres::{Client, NoTls};

/// Result type for SQL metadata operations.
pub type Result<T> = std::result::Result<T, SqlMetadataError>;

/// SQL metadata index errors.
#[derive(Debug, Error)]
pub enum SqlMetadataError {
    /// SQLite backend error.
    #[error("sqlite metadata error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// PostgreSQL backend error.
    #[cfg(feature = "postgres")]
    #[error("postgres metadata error: {0}")]
    Postgres(#[from] postgres::Error),

    /// Metadata JSON serialization/deserialization error.
    #[error("metadata JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Query could not be represented safely by the adapter.
    #[error("invalid metadata query: {0}")]
    InvalidQuery(String),

    /// Internal lock poisoned.
    #[error("metadata index lock poisoned: {0}")]
    Lock(String),
}

/// SQL backend names accepted by server configuration.
pub const SQLITE_BACKEND: &str = "sqlite";
/// PostgreSQL backend name accepted when the `postgres` feature is enabled.
pub const POSTGRES_BACKEND: &str = "postgres";

/// One chunk/vector metadata record mirrored into SQL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SqlMetadataRecord {
    /// AkiDB collection name.
    pub collection: String,
    /// External vector/chunk id.
    pub vector_id: String,
    /// Internal hot-index id, when available.
    pub internal_id: i64,
    /// Original JSON metadata.
    pub metadata: Value,
    /// Creation timestamp in milliseconds.
    pub created_at_ms: u64,
    /// Last update timestamp in milliseconds.
    pub updated_at_ms: u64,
}

impl SqlMetadataRecord {
    /// Build a SQL metadata record.
    pub fn new(
        collection: impl Into<String>,
        vector_id: impl Into<String>,
        internal_id: i64,
        metadata: Value,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            collection: collection.into(),
            vector_id: vector_id.into(),
            internal_id,
            metadata,
            created_at_ms,
            updated_at_ms,
        }
    }
}

/// Predicate over JSON metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataPredicate {
    /// `metadata[field] == value`
    Eq { field: String, value: Value },
    /// `metadata[field]` exists.
    Exists { field: String },
}

/// Structured metadata query supported by the adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataQuery {
    /// Collection to query.
    pub collection: String,
    /// AND-ed metadata predicates.
    pub predicates: Vec<MetadataPredicate>,
    /// Maximum ids to return.
    pub limit: usize,
}

impl MetadataQuery {
    /// Create a query for one collection.
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            predicates: Vec::new(),
            limit: 100,
        }
    }

    /// Add an equality predicate.
    pub fn with_eq(mut self, field: impl Into<String>, value: Value) -> Self {
        self.predicates.push(MetadataPredicate::Eq {
            field: field.into(),
            value,
        });
        self
    }

    /// Add an existence predicate.
    pub fn with_exists(mut self, field: impl Into<String>) -> Self {
        self.predicates.push(MetadataPredicate::Exists {
            field: field.into(),
        });
        self
    }

    /// Set a result limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Basic SQL metadata index stats.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqlMetadataStats {
    /// Number of active mirrored chunk records.
    pub records: u64,
}

/// Optional metadata SQL backend used by the retrieval planner.
pub trait MetadataSqlIndex: Send + Sync {
    /// Upsert one metadata record.
    fn upsert_record(&self, record: &SqlMetadataRecord) -> Result<()>;

    /// Delete a record by collection/vector id.
    fn delete_record(&self, collection: &str, vector_id: &str) -> Result<()>;

    /// Delete all mirrored records for a collection.
    fn clear_collection(&self, collection: &str) -> Result<()>;

    /// Query vector ids matching a structured metadata query.
    fn query_ids(&self, query: &MetadataQuery) -> Result<Vec<String>>;

    /// Return current backend stats.
    fn stats(&self) -> Result<SqlMetadataStats>;
}

/// SQLite implementation of the optional SQL metadata index.
pub struct SqliteMetadataIndex {
    conn: Mutex<Connection>,
}

impl SqliteMetadataIndex {
    /// Open or create a SQLite metadata index at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// Create an in-memory SQLite metadata index.
    pub fn in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS akidb_chunks (
                collection TEXT NOT NULL,
                vector_id TEXT NOT NULL,
                internal_id INTEGER NOT NULL,
                metadata_json TEXT NOT NULL,
                tenant_id TEXT,
                source TEXT,
                repo TEXT,
                file TEXT,
                language TEXT,
                updated_at TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (collection, vector_id)
            );
            CREATE INDEX IF NOT EXISTS idx_akidb_chunks_collection_updated
                ON akidb_chunks(collection, updated_at_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_akidb_chunks_tenant
                ON akidb_chunks(collection, tenant_id);
            CREATE INDEX IF NOT EXISTS idx_akidb_chunks_repo
                ON akidb_chunks(collection, repo);
            CREATE INDEX IF NOT EXISTS idx_akidb_chunks_file
                ON akidb_chunks(collection, file);
            ",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|e| SqlMetadataError::Lock(e.to_string()))
    }
}

impl MetadataSqlIndex for SqliteMetadataIndex {
    fn upsert_record(&self, record: &SqlMetadataRecord) -> Result<()> {
        let metadata_json = serde_json::to_string(&record.metadata)?;
        let tenant_id = metadata_string(&record.metadata, "tenant_id");
        let source = metadata_string(&record.metadata, "source");
        let repo = metadata_string(&record.metadata, "repo");
        let file = metadata_string(&record.metadata, "file");
        let language = metadata_string(&record.metadata, "language");
        let updated_at = metadata_string(&record.metadata, "updated_at");

        self.conn()?.execute(
            "
            INSERT INTO akidb_chunks (
                collection, vector_id, internal_id, metadata_json, tenant_id,
                source, repo, file, language, updated_at, created_at_ms, updated_at_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            ON CONFLICT(collection, vector_id) DO UPDATE SET
                internal_id = excluded.internal_id,
                metadata_json = excluded.metadata_json,
                tenant_id = excluded.tenant_id,
                source = excluded.source,
                repo = excluded.repo,
                file = excluded.file,
                language = excluded.language,
                updated_at = excluded.updated_at,
                updated_at_ms = excluded.updated_at_ms
            ",
            params![
                record.collection,
                record.vector_id,
                record.internal_id,
                metadata_json,
                tenant_id,
                source,
                repo,
                file,
                language,
                updated_at,
                record.created_at_ms as i64,
                record.updated_at_ms as i64,
            ],
        )?;
        Ok(())
    }

    fn delete_record(&self, collection: &str, vector_id: &str) -> Result<()> {
        self.conn()?.execute(
            "DELETE FROM akidb_chunks WHERE collection = ?1 AND vector_id = ?2",
            params![collection, vector_id],
        )?;
        Ok(())
    }

    fn clear_collection(&self, collection: &str) -> Result<()> {
        self.conn()?.execute(
            "DELETE FROM akidb_chunks WHERE collection = ?1",
            params![collection],
        )?;
        Ok(())
    }

    fn query_ids(&self, query: &MetadataQuery) -> Result<Vec<String>> {
        if query.collection.trim().is_empty() {
            return Err(SqlMetadataError::InvalidQuery(
                "collection cannot be empty".to_string(),
            ));
        }
        if query.limit == 0 {
            return Ok(Vec::new());
        }

        let mut sql = String::from(
            "SELECT vector_id FROM akidb_chunks WHERE collection = ? ORDER_PLACEHOLDER",
        );
        let mut params = vec![SqlValue::Text(query.collection.clone())];
        let mut clauses = Vec::new();

        for predicate in &query.predicates {
            match predicate {
                MetadataPredicate::Eq { field, value } => {
                    let path = json_path(field)?;
                    if value.is_null() {
                        clauses.push("json_type(metadata_json, ?) = 'null'".to_string());
                        params.push(SqlValue::Text(path));
                    } else {
                        clauses.push("json_extract(metadata_json, ?) = ?".to_string());
                        params.push(SqlValue::Text(path));
                        params.push(sql_value(value)?);
                    }
                }
                MetadataPredicate::Exists { field } => {
                    let path = json_path(field)?;
                    clauses.push("json_type(metadata_json, ?) IS NOT NULL".to_string());
                    params.push(SqlValue::Text(path));
                }
            }
        }

        if !clauses.is_empty() {
            sql = sql.replace(
                " ORDER_PLACEHOLDER",
                &format!(" AND {} ORDER_PLACEHOLDER", clauses.join(" AND ")),
            );
        }
        sql = sql.replace(
            " ORDER_PLACEHOLDER",
            " ORDER BY updated_at_ms DESC, vector_id ASC LIMIT ?",
        );
        params.push(SqlValue::Integer(query.limit.min(i64::MAX as usize) as i64));

        let conn = self.conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params), |row| row.get::<_, String>(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn stats(&self) -> Result<SqlMetadataStats> {
        let records = self
            .conn()?
            .query_row("SELECT COUNT(*) FROM akidb_chunks", [], |row| {
                row.get::<_, i64>(0)
            })?;
        Ok(SqlMetadataStats {
            records: records.max(0) as u64,
        })
    }
}

/// PostgreSQL implementation of the optional SQL metadata index.
#[cfg(feature = "postgres")]
pub struct PostgresMetadataIndex {
    client: Mutex<Client>,
}

#[cfg(feature = "postgres")]
impl PostgresMetadataIndex {
    /// Connect to PostgreSQL and initialize the metadata schema.
    pub fn connect(params: &str) -> Result<Self> {
        let mut client = Client::connect(params, NoTls)?;
        Self::initialize(&mut client)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    fn initialize(client: &mut Client) -> Result<()> {
        client.batch_execute(POSTGRES_SCHEMA_SQL)?;
        Ok(())
    }

    fn client(&self) -> Result<std::sync::MutexGuard<'_, Client>> {
        self.client
            .lock()
            .map_err(|e| SqlMetadataError::Lock(e.to_string()))
    }
}

#[cfg(feature = "postgres")]
impl MetadataSqlIndex for PostgresMetadataIndex {
    fn upsert_record(&self, record: &SqlMetadataRecord) -> Result<()> {
        let metadata_json = serde_json::to_string(&record.metadata)?;
        let tenant_id = metadata_string(&record.metadata, "tenant_id");
        let source = metadata_string(&record.metadata, "source");
        let repo = metadata_string(&record.metadata, "repo");
        let file = metadata_string(&record.metadata, "file");
        let language = metadata_string(&record.metadata, "language");
        let updated_at = metadata_string(&record.metadata, "updated_at");

        self.client()?.execute(
            "
            INSERT INTO akidb_chunks (
                collection, vector_id, internal_id, metadata_json, tenant_id,
                source, repo, file, language, updated_at, created_at_ms, updated_at_ms
            )
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT(collection, vector_id) DO UPDATE SET
                internal_id = EXCLUDED.internal_id,
                metadata_json = EXCLUDED.metadata_json,
                tenant_id = EXCLUDED.tenant_id,
                source = EXCLUDED.source,
                repo = EXCLUDED.repo,
                file = EXCLUDED.file,
                language = EXCLUDED.language,
                updated_at = EXCLUDED.updated_at,
                updated_at_ms = EXCLUDED.updated_at_ms
            ",
            &[
                &record.collection,
                &record.vector_id,
                &record.internal_id,
                &metadata_json,
                &tenant_id,
                &source,
                &repo,
                &file,
                &language,
                &updated_at,
                &(record.created_at_ms as i64),
                &(record.updated_at_ms as i64),
            ],
        )?;
        Ok(())
    }

    fn delete_record(&self, collection: &str, vector_id: &str) -> Result<()> {
        self.client()?.execute(
            "DELETE FROM akidb_chunks WHERE collection = $1 AND vector_id = $2",
            &[&collection, &vector_id],
        )?;
        Ok(())
    }

    fn clear_collection(&self, collection: &str) -> Result<()> {
        self.client()?.execute(
            "DELETE FROM akidb_chunks WHERE collection = $1",
            &[&collection],
        )?;
        Ok(())
    }

    fn query_ids(&self, query: &MetadataQuery) -> Result<Vec<String>> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let (sql, values, limit) = postgres_query_sql(query)?;
        let mut params: Vec<&(dyn ToSql + Sync)> = values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        params.push(&limit);

        let rows = self.client()?.query(&sql, &params)?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    }

    fn stats(&self) -> Result<SqlMetadataStats> {
        let row = self
            .client()?
            .query_one("SELECT COUNT(*) FROM akidb_chunks", &[])?;
        let records = row.get::<_, i64>(0);
        Ok(SqlMetadataStats {
            records: records.max(0) as u64,
        })
    }
}

/// PostgreSQL schema used by the optional metadata adapter.
#[cfg(feature = "postgres")]
pub const POSTGRES_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS akidb_chunks (
    collection TEXT NOT NULL,
    vector_id TEXT NOT NULL,
    internal_id BIGINT NOT NULL,
    metadata_json JSONB NOT NULL,
    tenant_id TEXT,
    source TEXT,
    repo TEXT,
    file TEXT,
    language TEXT,
    updated_at TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (collection, vector_id)
);
CREATE INDEX IF NOT EXISTS idx_akidb_chunks_collection_updated
    ON akidb_chunks(collection, updated_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_akidb_chunks_tenant
    ON akidb_chunks(collection, tenant_id);
CREATE INDEX IF NOT EXISTS idx_akidb_chunks_repo
    ON akidb_chunks(collection, repo);
CREATE INDEX IF NOT EXISTS idx_akidb_chunks_file
    ON akidb_chunks(collection, file);
CREATE INDEX IF NOT EXISTS idx_akidb_chunks_metadata_gin
    ON akidb_chunks USING GIN (metadata_json);
"#;

fn metadata_string(metadata: &Value, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn json_path(field: &str) -> Result<String> {
    if field.trim().is_empty() {
        return Err(SqlMetadataError::InvalidQuery(
            "metadata field cannot be empty".to_string(),
        ));
    }
    let parts: Vec<&str> = field.split('.').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || !part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return Err(SqlMetadataError::InvalidQuery(format!(
            "unsupported metadata field path '{field}'"
        )));
    }
    Ok(format!("$.{}", parts.join(".")))
}

#[cfg(feature = "postgres")]
fn postgres_json_path(field: &str) -> Result<String> {
    let path = json_path(field)?;
    Ok(format!(
        "'{{{}}}'",
        path.trim_start_matches("$.").replace('.', ",")
    ))
}

fn sql_value(value: &Value) -> Result<SqlValue> {
    match value {
        Value::String(s) => Ok(SqlValue::Text(s.clone())),
        Value::Bool(v) => Ok(SqlValue::Integer(i64::from(*v))),
        Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                Ok(SqlValue::Integer(v))
            } else if let Some(v) = n.as_u64() {
                if v > i64::MAX as u64 {
                    return Err(SqlMetadataError::InvalidQuery(format!(
                        "numeric metadata value {v} exceeds SQLite integer range"
                    )));
                }
                Ok(SqlValue::Integer(v as i64))
            } else if let Some(v) = n.as_f64() {
                Ok(SqlValue::Real(v))
            } else {
                Err(SqlMetadataError::InvalidQuery(
                    "unsupported numeric metadata value".to_string(),
                ))
            }
        }
        Value::Null => Ok(SqlValue::Null),
        Value::Array(_) | Value::Object(_) => Err(SqlMetadataError::InvalidQuery(
            "metadata equality only supports scalar JSON values".to_string(),
        )),
    }
}

#[cfg(feature = "postgres")]
fn postgres_scalar_value(value: &Value) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(SqlMetadataError::InvalidQuery(
            "PostgreSQL metadata equality only supports non-null scalar JSON values".to_string(),
        )),
    }
}

#[cfg(feature = "postgres")]
fn postgres_query_sql(query: &MetadataQuery) -> Result<(String, Vec<String>, i64)> {
    if query.collection.trim().is_empty() {
        return Err(SqlMetadataError::InvalidQuery(
            "collection cannot be empty".to_string(),
        ));
    }
    let mut sql = String::from("SELECT vector_id FROM akidb_chunks WHERE collection = $1");
    let mut values = vec![query.collection.clone()];
    let mut param_index = 2usize;

    for predicate in &query.predicates {
        match predicate {
            MetadataPredicate::Eq { field, value } => {
                let path = postgres_json_path(field)?;
                if value.is_null() {
                    sql.push_str(&format!(" AND metadata_json #> {path} = 'null'::jsonb"));
                } else {
                    sql.push_str(&format!(" AND metadata_json #>> {path} = ${param_index}"));
                    values.push(postgres_scalar_value(value)?);
                    param_index += 1;
                }
            }
            MetadataPredicate::Exists { field } => {
                let path = postgres_json_path(field)?;
                sql.push_str(&format!(" AND metadata_json #> {path} IS NOT NULL"));
            }
        }
    }

    sql.push_str(&format!(
        " ORDER BY updated_at_ms DESC, vector_id ASC LIMIT ${param_index}"
    ));
    Ok((sql, values, query.limit.min(i64::MAX as usize) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_upsert_query_and_delete_metadata() {
        let index = SqliteMetadataIndex::in_memory().unwrap();
        index
            .upsert_record(&SqlMetadataRecord::new(
                "test",
                "chunk-a",
                1,
                json!({
                    "tenant_id": "defai",
                    "repo": "ax-engine",
                    "file": "mtp_scheduler.rs",
                    "year": 2026,
                    "active": true
                }),
                10,
                20,
            ))
            .unwrap();
        index
            .upsert_record(&SqlMetadataRecord::new(
                "test",
                "chunk-b",
                2,
                json!({"tenant_id": "other", "repo": "ax-engine"}),
                10,
                30,
            ))
            .unwrap();

        let ids = index
            .query_ids(
                &MetadataQuery::new("test")
                    .with_eq("tenant_id", json!("defai"))
                    .with_eq("year", json!(2026))
                    .with_eq("active", json!(true))
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(ids, vec!["chunk-a"]);

        let stats = index.stats().unwrap();
        assert_eq!(stats.records, 2);

        index.delete_record("test", "chunk-a").unwrap();
        let ids = index
            .query_ids(
                &MetadataQuery::new("test")
                    .with_eq("tenant_id", json!("defai"))
                    .with_limit(10),
            )
            .unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_exists_predicate() {
        let index = SqliteMetadataIndex::in_memory().unwrap();
        index
            .upsert_record(&SqlMetadataRecord::new(
                "test",
                "chunk-a",
                1,
                json!({"repo": "ax-engine"}),
                10,
                20,
            ))
            .unwrap();

        let ids = index
            .query_ids(
                &MetadataQuery::new("test")
                    .with_exists("repo")
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(ids, vec!["chunk-a"]);
    }

    #[test]
    fn test_null_equality_predicate_matches_json_null() {
        let index = SqliteMetadataIndex::in_memory().unwrap();
        index
            .upsert_record(&SqlMetadataRecord::new(
                "test",
                "chunk-null",
                1,
                json!({"deleted_at": null}),
                10,
                20,
            ))
            .unwrap();
        index
            .upsert_record(&SqlMetadataRecord::new(
                "test",
                "chunk-missing",
                2,
                json!({"repo": "ax-engine"}),
                10,
                30,
            ))
            .unwrap();

        let ids = index
            .query_ids(
                &MetadataQuery::new("test")
                    .with_eq("deleted_at", json!(null))
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(ids, vec!["chunk-null"]);
    }

    #[test]
    fn test_rejects_unsafe_field_paths() {
        let index = SqliteMetadataIndex::in_memory().unwrap();
        let err = index
            .query_ids(
                &MetadataQuery::new("test")
                    .with_exists("repo;DROP")
                    .with_limit(10),
            )
            .unwrap_err();
        assert!(matches!(err, SqlMetadataError::InvalidQuery(_)));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_query_sql_uses_safe_json_paths_and_parameters() {
        let (sql, values, limit) = postgres_query_sql(
            &MetadataQuery::new("test")
                .with_eq("tenant_id", json!("defai"))
                .with_eq("nested.year", json!(2026))
                .with_exists("repo")
                .with_limit(25),
        )
        .unwrap();

        assert_eq!(values, vec!["test", "defai", "2026"]);
        assert_eq!(limit, 25);
        assert!(sql.contains("collection = $1"));
        assert!(sql.contains("metadata_json #>> '{tenant_id}' = $2"));
        assert!(sql.contains("metadata_json #>> '{nested,year}' = $3"));
        assert!(sql.contains("metadata_json #> '{repo}' IS NOT NULL"));
        assert!(sql.contains("LIMIT $4"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_query_sql_matches_json_null() {
        let (sql, values, limit) = postgres_query_sql(
            &MetadataQuery::new("test")
                .with_eq("tenant_id", json!(null))
                .with_limit(1),
        )
        .unwrap();

        assert_eq!(values, vec!["test"]);
        assert_eq!(limit, 1);
        assert!(sql.contains("metadata_json #> '{tenant_id}' = 'null'::jsonb"));
        assert!(sql.contains("LIMIT $2"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_rejects_unsafe_fields() {
        let err = postgres_query_sql(
            &MetadataQuery::new("test")
                .with_exists("tenant_id;drop")
                .with_limit(1),
        )
        .unwrap_err();
        assert!(matches!(err, SqlMetadataError::InvalidQuery(_)));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_schema_has_jsonb_and_indexes() {
        assert!(POSTGRES_SCHEMA_SQL.contains("metadata_json JSONB NOT NULL"));
        assert!(POSTGRES_SCHEMA_SQL.contains("USING GIN (metadata_json)"));
        assert!(POSTGRES_SCHEMA_SQL.contains("PRIMARY KEY (collection, vector_id)"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_query_sql_limit_zero_is_not_built_for_execution() {
        let (sql, values, limit) =
            postgres_query_sql(&MetadataQuery::new("test").with_limit(0)).unwrap();
        assert!(sql.contains("LIMIT $2"));
        assert_eq!(values, vec!["test"]);
        assert_eq!(limit, 0);
    }
}

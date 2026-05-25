use std::cell::{Cell, RefCell};

use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use akidb_native::{
    AkiDbError, Collection, EngineInner, EngineOptions, Manifest, NativeRecord, SearchMode,
    SearchOptions,
};

// ─── Error conversion ────────────────────────────────────────────────────────

fn to_py_err(e: AkiDbError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ─── PyO3 classes ────────────────────────────────────────────────────────────

/// AkiDB — embedded vector database engine.
///
/// Use as a context manager for automatic cleanup:
///
///     with AkiDB("/path/to/db") as db:
///         db.create_collection(...)
///         db.upsert_batch(...)
///         db.publish(...)
///         results = db.search(...)
#[pyclass(unsendable)]
struct AkiDB {
    inner: RefCell<EngineInner>,
    /// Guard against double-close (RefCell borrow_mut would panic on second call).
    closed: Cell<bool>,
}

#[pymethods]
impl AkiDB {
    #[new]
    #[pyo3(signature = (storage_path, disable_wal=false))]
    fn new(storage_path: String, disable_wal: bool) -> PyResult<Self> {
        let engine = EngineInner::open(EngineOptions {
            storage_path: storage_path.into(),
            disable_wal,
        })
        .map_err(to_py_err)?;

        Ok(Self {
            inner: RefCell::new(engine),
            closed: Cell::new(false),
        })
    }

    // ─── Collection Management ──────────────────────────────────────────

    /// Create a new vector collection.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        collection_id,
        dimension,
        metric,
        embedding_model_id,
        quantization="fp16",
        hnsw_m=16,
        hnsw_ef_construction=200,
        hnsw_ef_search=100,
    ))]
    fn create_collection<'py>(
        &self,
        py: Python<'py>,
        collection_id: &str,
        dimension: i64,
        metric: &str,
        embedding_model_id: &str,
        quantization: &str,
        hnsw_m: i64,
        hnsw_ef_construction: i64,
        hnsw_ef_search: i64,
    ) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow();
        let c = inner
            .create_collection(
                collection_id,
                dimension,
                metric,
                embedding_model_id,
                quantization,
                hnsw_m,
                hnsw_ef_construction,
                hnsw_ef_search,
            )
            .map_err(to_py_err)?;
        collection_to_dict(py, c)
    }

    /// Get a collection by ID. Returns None if not found.
    fn get_collection<'py>(&self, py: Python<'py>, collection_id: &str) -> PyResult<Option<Py<PyDict>>> {
        let inner = self.inner.borrow();
        let c = inner.get_collection(collection_id).map_err(to_py_err)?;
        match c {
            Some(c) => Ok(Some(collection_to_dict(py, c)?)),
            None => Ok(None),
        }
    }

    /// List all collections.
    fn list_collections<'py>(&self, py: Python<'py>) -> PyResult<Py<PyList>> {
        let inner = self.inner.borrow();
        let collections = inner.list_collections().map_err(to_py_err)?;
        let list = PyList::empty(py);
        for c in collections {
            list.append(collection_to_dict(py, c)?)?;
        }
        Ok(list.into())
    }

    /// Delete a collection.
    fn delete_collection(&self, collection_id: &str) -> PyResult<()> {
        let inner = self.inner.borrow();
        inner.delete_collection(collection_id).map_err(to_py_err)
    }

    // ─── Write ──────────────────────────────────────────────────────────

    /// Upsert a batch of records into a collection.
    ///
    /// Each record is a dict with keys: chunk_id (str), doc_id (str),
    /// vector (list[float]), and optionally metadata (dict) and chunk_text (str).
    fn upsert_batch<'py>(
        &self,
        py: Python<'py>,
        collection_id: &str,
        records: Vec<PyObject>,
    ) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow_mut();

        let mut native_records: Vec<NativeRecord> = Vec::with_capacity(records.len());
        for (i, rec_obj) in records.iter().enumerate() {
            let rec = rec_obj.bind(py).downcast::<PyDict>().map_err(|_| {
                PyRuntimeError::new_err("Each record must be a dict")
            })?;

            let chunk_id: String = rec.get_item("chunk_id")
                .map_err(|_| PyKeyError::new_err("Record missing 'chunk_id'"))?
                .ok_or_else(|| PyKeyError::new_err("Record missing 'chunk_id'"))?
                .extract()?;
            let doc_id: String = rec.get_item("doc_id")
                .map_err(|_| PyKeyError::new_err("Record missing 'doc_id'"))?
                .ok_or_else(|| PyKeyError::new_err("Record missing 'doc_id'"))?
                .extract()?;
            let vector: Vec<f64> = rec.get_item("vector")
                .map_err(|_| PyKeyError::new_err("Record missing 'vector'"))?
                .ok_or_else(|| PyKeyError::new_err("Record missing 'vector'"))?
                .extract()?;

            let metadata = match rec.get_item("metadata")? {
                Some(m) => {
                    if m.is_instance_of::<PyDict>() {
                        let json_mod = py.import("json")?;
                        let py_str: String = json_mod.call_method1("dumps", (m,))?.extract()?;
                        serde_json::from_str(&py_str).map_err(|e| {
                            PyValueError::new_err(format!("record[{i}] metadata is not valid JSON: {e}"))
                        })?
                    } else {
                        let s: String = m.str()?.extract()?;
                        serde_json::from_str(&s).map_err(|e| {
                            PyValueError::new_err(format!("record[{i}] metadata string is not valid JSON: {e}"))
                        })?
                    }
                }
                None => serde_json::json!({}),
            };

            let chunk_text = match rec.get_item("chunk_text")? {
                Some(text_obj) => {
                    let text: String = text_obj.extract()?;
                    if text.is_empty() { None } else { Some(text) }
                }
                None => None,
            };

            native_records.push(NativeRecord {
                chunk_id,
                doc_id,
                vector: vector.into_iter().map(|v| v as f32).collect(),
                metadata,
                chunk_text,
            });
        }

        let result = inner.upsert_batch(collection_id, &native_records).map_err(to_py_err)?;

        let dict = PyDict::new(py);
        dict.set_item("segment_ids", &result.segment_ids)?;
        dict.set_item("buffered_count", result.buffered_count)?;
        Ok(dict.into())
    }

    /// Flush in-memory write buffer to disk segments.
    fn flush_writes(&self, collection_id: &str) -> PyResult<Vec<String>> {
        let inner = self.inner.borrow_mut();
        inner.flush_writes(collection_id).map_err(to_py_err)
    }

    // ─── Publish ────────────────────────────────────────────────────────

    /// Publish a new manifest (snapshot) for the collection.
    fn publish<'py>(
        &self,
        py: Python<'py>,
        collection_id: &str,
        embedding_model_id: &str,
        pipeline_signature: &str,
    ) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow_mut();
        let m = inner
            .auto_publish(collection_id, embedding_model_id, pipeline_signature)
            .map_err(to_py_err)?;
        manifest_to_dict(py, m)
    }

    // ─── Search ─────────────────────────────────────────────────────────

    /// Search a collection. Returns dict with 'results', 'manifest_version_used', and 'telemetry'.
    #[pyo3(signature = (
        collection_id,
        query_vector,
        top_k=10,
        filters=None,
        manifest_version=None,
        include_uncommitted=true,
        mode="vector",
        query_text=None,
        vector_weight=1.0,
        keyword_weight=1.0,
        explain=false,
        ef_search=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn search<'py>(
        &self,
        py: Python<'py>,
        collection_id: String,
        query_vector: Vec<f32>,
        top_k: usize,
        filters: Option<PyObject>,
        manifest_version: Option<i64>,
        include_uncommitted: bool,
        mode: &str,
        query_text: Option<String>,
        vector_weight: f64,
        keyword_weight: f64,
        explain: bool,
        ef_search: Option<usize>,
    ) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow();

        let parsed_filters = match filters {
            Some(obj) => {
                let bound = obj.bind(py);
                let json_str = if bound.is_instance_of::<PyDict>() {
                    // Accept a Python dict — serialize it to JSON for the engine
                    let json_mod = py.import("json")?;
                    json_mod.call_method1("dumps", (bound,))?.extract::<String>()?
                } else {
                    // Accept a pre-serialized JSON string
                    bound.extract::<String>()?
                };
                Some(
                    serde_json::from_str(&json_str)
                        .map_err(|e| PyRuntimeError::new_err(format!("Invalid filters JSON: {e}")))?,
                )
            }
            None => None,
        };

        let opts = SearchOptions {
            collection_id,
            query_vector,
            top_k,
            filters: parsed_filters,
            manifest_version,
            include_uncommitted,
            mode: SearchMode::from_str(mode),
            query_text,
            vector_weight,
            keyword_weight,
            explain,
            ef_search,
        };

        let response = inner.search(opts).map_err(to_py_err)?;

        let dict = PyDict::new(py);
        let results = PyList::empty(py);
        for r in &response.results {
            let rd = PyDict::new(py);
            rd.set_item("chunk_id", &r.chunk_id)?;
            rd.set_item("score", r.score)?;
            if let Some(committed) = r.committed {
                rd.set_item("committed", committed)?;
            }
            if let Some(ref e) = r.explain {
                let ed = PyDict::new(py);
                if let Some(vs) = e.vector_score { ed.set_item("vector_score", vs)?; }
                if let Some(bs) = e.bm25_score { ed.set_item("bm25_score", bs)?; }
                if let Some(rs) = e.rrf_score { ed.set_item("rrf_score", rs)?; }
                if let Some(vr) = e.vector_rank { ed.set_item("vector_rank", vr)?; }
                if let Some(br) = e.bm25_rank { ed.set_item("bm25_rank", br)?; }
                if let Some(ref cp) = e.chunk_preview { ed.set_item("chunk_preview", cp)?; }
                ed.set_item("matched_terms", &e.matched_terms)?;
                rd.set_item("explain", ed)?;
            }
            results.append(rd)?;
        }
        dict.set_item("results", results)?;
        dict.set_item("manifest_version_used", response.manifest_version_used)?;

        Ok(dict.into())
    }

    // ─── Delete ─────────────────────────────────────────────────────────

    /// Delete chunks by ID. Returns number of tombstones created.
    #[pyo3(signature = (collection_id, chunk_ids, reason="manual_revoke"))]
    fn delete_chunks(
        &self,
        collection_id: &str,
        chunk_ids: Vec<String>,
        reason: &str,
    ) -> PyResult<usize> {
        let inner = self.inner.borrow();
        inner
            .delete_chunks(collection_id, &chunk_ids, reason)
            .map_err(to_py_err)
    }

    // ─── Compact ────────────────────────────────────────────────────────

    /// Compact a collection: merge segments, apply tombstones.
    fn compact<'py>(&self, py: Python<'py>, collection_id: &str) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow();
        let result = inner.compact(collection_id).map_err(to_py_err)?;

        let dict = PyDict::new(py);
        dict.set_item("records_kept", result.records_kept)?;
        dict.set_item("records_removed", result.records_removed)?;
        dict.set_item("space_reclaimed_bytes", result.space_reclaimed_bytes)?;
        dict.set_item("manifest", manifest_to_dict(py, result.manifest)?)?;
        Ok(dict.into())
    }

    /// Compatibility shim: currently executes compaction synchronously.
    fn compact_async(&self, collection_id: &str) -> PyResult<()> {
        let inner = self.inner.borrow();
        let _ = inner.compact(collection_id).map_err(to_py_err)?;
        Ok(())
    }

    /// Compatibility shim until native async status is available.
    fn compaction_status<'py>(&self, py: Python<'py>, collection_id: &str) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow();
        let _ = inner
            .get_collection(collection_id)
            .map_err(to_py_err)?
            .ok_or_else(|| PyRuntimeError::new_err(format!("collection not found: {collection_id}")))?;
        let dict = PyDict::new(py);
        dict.set_item("pending", false)?;
        dict.set_item("last_compacted_at", py.None())?;
        dict.set_item("segment_count", 0)?;
        Ok(dict.into())
    }

    // ─── Rollback ───────────────────────────────────────────────────────

    /// Rollback to a previous manifest version.
    fn rollback<'py>(
        &self,
        py: Python<'py>,
        collection_id: &str,
        manifest_id: &str,
    ) -> PyResult<Py<PyDict>> {
        let inner = self.inner.borrow();
        let m = inner
            .rollback(collection_id, manifest_id)
            .map_err(to_py_err)?;
        manifest_to_dict(py, m)
    }

    // ─── Introspection ──────────────────────────────────────────────────

    /// Get total storage size in bytes.
    fn get_storage_size_bytes(&self) -> PyResult<u64> {
        let inner = self.inner.borrow();
        inner.get_storage_size_bytes().map_err(to_py_err)
    }

    /// Get number of active tombstones for a collection.
    fn get_tombstone_count(&self, collection_id: &str) -> PyResult<i64> {
        let inner = self.inner.borrow();
        inner.get_tombstone_count(collection_id).map_err(to_py_err)
    }

    // ─── Close ──────────────────────────────────────────────────────────

    /// Close the engine and release resources. Safe to call multiple times.
    fn close(&self) -> PyResult<()> {
        if self.closed.get() {
            return Ok(());
        }
        self.closed.set(true);
        let inner = self.inner.borrow_mut();
        inner.close().map_err(to_py_err)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, pyo3::types::PyType>>,
        _exc_val: Option<&Bound<'_, pyo3::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::PyAny>>,
    ) -> PyResult<bool> {
        // Best-effort close: don't mask the original exception if one is propagating.
        let _ = self.close();
        Ok(false)
    }
}

// ─── Conversion helpers ──────────────────────────────────────────────────────

fn collection_to_dict(py: Python<'_>, c: Collection) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("collection_id", &c.collection_id)?;
    d.set_item("dimension", c.dimension)?;
    d.set_item("metric", &c.metric)?;
    d.set_item("embedding_model_id", &c.embedding_model_id)?;
    d.set_item("schema_version", &c.schema_version)?;
    d.set_item("created_at", &c.created_at)?;
    d.set_item("deleted_at", &c.deleted_at)?;
    d.set_item("quantization", &c.quantization)?;
    d.set_item("hnsw_m", c.hnsw_m)?;
    d.set_item("hnsw_ef_construction", c.hnsw_ef_construction)?;
    d.set_item("hnsw_ef_search", c.hnsw_ef_search)?;
    Ok(d.into())
}

fn manifest_to_dict(py: Python<'_>, m: Manifest) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("manifest_id", &m.manifest_id)?;
    d.set_item("collection_id", &m.collection_id)?;
    d.set_item("version", m.version)?;
    d.set_item("segment_ids", &m.segment_ids)?;
    d.set_item("tombstone_ids", &m.tombstone_ids)?;
    d.set_item("embedding_model_id", &m.embedding_model_id)?;
    d.set_item("pipeline_signature", &m.pipeline_signature)?;
    d.set_item("created_at", &m.created_at)?;
    d.set_item("checksum", &m.checksum)?;
    Ok(d.into())
}

// ─── Module ──────────────────────────────────────────────────────────────────

/// AkiDB — embedded vector database for Python (Rust-powered).
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AkiDB>()?;
    Ok(())
}

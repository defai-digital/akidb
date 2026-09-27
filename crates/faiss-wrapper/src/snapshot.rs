//! Durable USearch HNSW snapshot.
//!
//! The graph file is USearch's own format. The manifest, id map, and tombstone
//! bitset are AkiDB's, because USearch does not store external ids or deletes.
//! A payload fingerprint covers external id, internal id, and raw f32 bits.
//! Metadata stays in the payload store and is intentionally outside the
//! fingerprint: changing it must not force a graph rebuild, while changing a
//! vector or its internal id must.

use crate::hnsw::{DistanceMetric, HnswConfig, VectorPrecision};
use crate::tombstone::TombstoneBitset;
use crate::{AkiDbError, Result, VectorId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use usearch::Index;

/// Directory name used for a snapshot stored beside a generation or RocksDB path.
pub const HNSW_SNAPSHOT_DIR: &str = "hnsw-snapshot";

const SCHEMA_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "manifest.json";
const GRAPH_FILE: &str = "graph.usearch";
const MAPPINGS_FILE: &str = "mappings.bin";
const TOMBSTONES_FILE: &str = "tombstones.bin";
const EXACT_VECTORS_FILE: &str = "exact-f32.bin";

/// One active durable vector, borrowed from the payload store.
#[derive(Debug, Clone, Copy)]
pub struct DurableVectorRef<'a> {
    pub external_id: &'a str,
    pub internal_id: i64,
    pub vector: &'a [f32],
}

/// Canonical fingerprint of the vectors a snapshot is allowed to serve.
///
/// Order does not matter. Two snapshots of the same multiset of
/// `(external id, internal id, f32 bits)` produce the same digest.
pub fn durable_vector_fingerprint(records: &[DurableVectorRef<'_>]) -> String {
    let mut order: Vec<usize> = (0..records.len()).collect();
    order.sort_by(|&left, &right| {
        records[left]
            .external_id
            .cmp(records[right].external_id)
            .then(records[left].internal_id.cmp(&records[right].internal_id))
    });

    let mut hasher = Sha256::new();
    hasher.update(b"akidb-hnsw-payload-v1");
    hasher.update((records.len() as u64).to_be_bytes());
    for index in order {
        let record = &records[index];
        hasher.update((record.external_id.len() as u64).to_be_bytes());
        hasher.update(record.external_id.as_bytes());
        hasher.update(record.internal_id.to_be_bytes());
        hasher.update((record.vector.len() as u64).to_be_bytes());
        for value in record.vector {
            hasher.update(value.to_bits().to_be_bytes());
        }
    }
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotManifest {
    schema_version: u32,
    dimensions: usize,
    connectivity: usize,
    ef_construction: usize,
    /// Search breadth recorded at save time. Load applies the caller's
    /// `ef_search` instead, so operators can retune search without a rebuild.
    ef_search: usize,
    precision: String,
    metric: String,
    next_id: i64,
    vector_count: u64,
    tombstone_capacity: u64,
    deleted_count: u64,
    payload_sha256: String,
    graph_sha256: String,
    mappings_sha256: String,
    tombstones_sha256: String,
    /// SHA-256 of `exact-f32.bin` when routing uses I8. Empty for f32 and f16.
    #[serde(default)]
    exact_sha256: String,
}

pub(crate) struct LoadedSnapshot {
    pub index: Index,
    pub id_mapping: HashMap<String, i64>,
    pub reverse_mapping: HashMap<i64, VectorId>,
    pub tombstones: TombstoneBitset,
    pub next_id: i64,
    pub exact_vectors: Vec<Option<Vec<f32>>>,
}

pub(crate) struct SnapshotSource<'a> {
    pub index: &'a Index,
    pub id_mapping: &'a HashMap<String, i64>,
    pub reverse_mapping: &'a HashMap<i64, VectorId>,
    pub tombstones: &'a TombstoneBitset,
    pub next_id: i64,
    pub dimensions: usize,
    pub ef_search: usize,
    pub metric: DistanceMetric,
    pub exact_vectors: &'a [Option<Vec<f32>>],
}

pub(crate) fn save(
    source: &SnapshotSource<'_>,
    directory: &Path,
    payload_sha256: &str,
) -> Result<()> {
    reject_symlink(directory)?;
    let parent = directory
        .parent()
        .filter(|path| !path.as_os_str().is_empty());
    let staging_name = format!(
        "{}.partial-{}",
        directory
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(HNSW_SNAPSHOT_DIR),
        std::process::id()
    );
    let staging = match parent {
        Some(parent) => parent.join(staging_name),
        None => PathBuf::from(staging_name),
    };
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|error| io_error("remove previous partial HNSW snapshot", &staging, error))?;
    }
    let staged = StagedDir(staging);
    fs::create_dir_all(&staged.0)
        .map_err(|error| io_error("create partial HNSW snapshot", &staged.0, error))?;

    let mappings = encode_mappings(
        source.id_mapping,
        source.reverse_mapping,
        source.index,
        source.next_id,
    )?;
    let tombstone_bytes = source.tombstones.as_slice();
    if u64::try_from(tombstone_bytes.len()).ok() != Some(source.tombstones.capacity().div_ceil(8)) {
        return Err(AkiDbError::IndexError(
            "HNSW tombstone byte length does not match its capacity".to_string(),
        ));
    }

    let graph_path = staged.0.join(GRAPH_FILE);
    let graph_path_str = path_str(&graph_path)?;
    source.index.save(graph_path_str).map_err(|error| {
        AkiDbError::IndexError(format!("failed to save USearch graph: {error}"))
    })?;
    write_bytes(&staged.0.join(MAPPINGS_FILE), &mappings)?;
    write_bytes(&staged.0.join(TOMBSTONES_FILE), &tombstone_bytes)?;
    fsync_file(&graph_path)?;
    fsync_file(&staged.0.join(MAPPINGS_FILE))?;
    fsync_file(&staged.0.join(TOMBSTONES_FILE))?;
    let precision = precision_name(scalar_precision(source.index)?);
    let exact_bytes = if precision == "i8" {
        Some(encode_exact(source)?)
    } else {
        None
    };
    if let Some(exact_bytes) = &exact_bytes {
        let exact_path = staged.0.join(EXACT_VECTORS_FILE);
        write_bytes(&exact_path, exact_bytes)?;
        fsync_file(&exact_path)?;
    }

    let manifest = SnapshotManifest {
        schema_version: SCHEMA_VERSION,
        dimensions: source.dimensions,
        connectivity: source.index.connectivity(),
        ef_construction: source.index.expansion_add(),
        ef_search: source.ef_search,
        precision: precision.to_string(),
        metric: metric_name(source.metric).to_string(),
        next_id: source.next_id,
        vector_count: source.index.size() as u64,
        tombstone_capacity: source.tombstones.capacity(),
        deleted_count: source.tombstones.deleted_count(),
        payload_sha256: payload_sha256.to_string(),
        graph_sha256: sha256_file(&graph_path)?,
        mappings_sha256: sha256_bytes(&mappings),
        tombstones_sha256: sha256_bytes(&tombstone_bytes),
        exact_sha256: exact_bytes
            .as_ref()
            .map(|bytes| sha256_bytes(bytes))
            .unwrap_or_default(),
    };
    if source.index.metric_kind() != source.metric.to_metric_kind() {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot metric does not match the live index".to_string(),
        ));
    }
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| AkiDbError::SerializationError(error.to_string()))?;
    write_bytes(&staged.0.join(MANIFEST_FILE), &manifest_bytes)?;
    fsync_file(&staged.0.join(MANIFEST_FILE))?;
    fsync_dir(&staged.0)?;

    publish_directory(&staged.0, directory)?;
    Ok(())
}

pub(crate) fn load(
    directory: &Path,
    expected: &HnswConfig,
    payload_sha256: &str,
) -> Result<LoadedSnapshot> {
    reject_symlink(directory)?;
    let manifest = read_manifest(&directory.join(MANIFEST_FILE))?;
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(AkiDbError::IndexError(format!(
            "unsupported HNSW snapshot schema {}",
            manifest.schema_version
        )));
    }
    if manifest.dimensions != expected.dimensions
        || manifest.connectivity != expected.m
        || manifest.ef_construction != expected.ef_construction
        || manifest.precision != precision_name(expected.precision)
        || manifest.metric != metric_name(expected.metric)
    {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot configuration does not match the requested index".to_string(),
        ));
    }
    if manifest.payload_sha256 != payload_sha256 {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot payload fingerprint does not match durable vectors".to_string(),
        ));
    }
    if manifest.next_id < 0 {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot next id is negative".to_string(),
        ));
    }

    let graph_bytes = read_file(&directory.join(GRAPH_FILE))?;
    let mapping_bytes = read_file(&directory.join(MAPPINGS_FILE))?;
    let tombstone_bytes = read_file(&directory.join(TOMBSTONES_FILE))?;
    if sha256_bytes(&graph_bytes) != manifest.graph_sha256
        || sha256_bytes(&mapping_bytes) != manifest.mappings_sha256
        || sha256_bytes(&tombstone_bytes) != manifest.tombstones_sha256
    {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot checksum mismatch".to_string(),
        ));
    }

    let graph_path = directory.join(GRAPH_FILE);
    let index = Index::restore(path_str(&graph_path)?).map_err(|error| {
        AkiDbError::IndexError(format!("failed to restore USearch graph: {error}"))
    })?;
    if index.dimensions() != expected.dimensions
        || index.size() as u64 != manifest.vector_count
        || index.metric_kind() != expected.metric.to_metric_kind()
        || index.scalar_kind() != expected.precision.to_scalar_kind()
    {
        return Err(AkiDbError::IndexError(
            "restored USearch graph does not match the HNSW snapshot manifest".to_string(),
        ));
    }
    if index.connectivity() != 0 && index.connectivity() != expected.m {
        return Err(AkiDbError::IndexError(format!(
            "restored USearch connectivity {} does not match requested M {}",
            index.connectivity(),
            expected.m
        )));
    }
    index.change_expansion_search(expected.ef_search);

    let mappings: Vec<(String, i64)> = bincode::deserialize(&mapping_bytes)
        .map_err(|error| AkiDbError::SerializationError(error.to_string()))?;
    if mappings.len() as u64 != manifest.vector_count {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot mapping count does not match the graph".to_string(),
        ));
    }
    let mut id_mapping = HashMap::with_capacity(mappings.len());
    let mut reverse_mapping = HashMap::with_capacity(mappings.len());
    let mut seen_internal = HashSet::with_capacity(mappings.len());
    for (external_id, internal_id) in mappings {
        if external_id.is_empty() || internal_id < 0 || internal_id >= manifest.next_id {
            return Err(AkiDbError::IndexError(format!(
                "HNSW snapshot mapping for '{external_id}' is outside the id space"
            )));
        }
        if !seen_internal.insert(internal_id) || id_mapping.contains_key(&external_id) {
            return Err(AkiDbError::IndexError(
                "HNSW snapshot contains a duplicate id mapping".to_string(),
            ));
        }
        if !index.contains(internal_id as u64) {
            return Err(AkiDbError::IndexError(format!(
                "HNSW snapshot mapping {internal_id} is missing from the graph"
            )));
        }
        reverse_mapping.insert(internal_id, VectorId::new(&external_id));
        id_mapping.insert(external_id, internal_id);
    }

    let tombstones = TombstoneBitset::try_restore(
        manifest.tombstone_capacity,
        manifest.deleted_count,
        tombstone_bytes,
    )?;
    if tombstones.deleted_count() > manifest.vector_count {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot has more tombstones than vectors".to_string(),
        ));
    }
    for internal_id in reverse_mapping.keys() {
        if *internal_id as u64 >= tombstones.capacity() {
            return Err(AkiDbError::IndexError(
                "HNSW snapshot tombstone capacity does not cover every vector".to_string(),
            ));
        }
    }

    let exact_vectors = if manifest.precision == "i8" {
        if manifest.exact_sha256.is_empty() {
            return Err(AkiDbError::IndexError(
                "I8 HNSW snapshot is missing exact f32 vectors".to_string(),
            ));
        }
        let exact_bytes = read_file(&directory.join(EXACT_VECTORS_FILE))?;
        if sha256_bytes(&exact_bytes) != manifest.exact_sha256 {
            return Err(AkiDbError::IndexError(
                "HNSW snapshot checksum mismatch".to_string(),
            ));
        }
        decode_exact(&exact_bytes, &id_mapping, manifest.dimensions)?
    } else if !manifest.exact_sha256.is_empty() {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot has exact vectors for a non-i8 index".to_string(),
        ));
    } else {
        Vec::new()
    };

    Ok(LoadedSnapshot {
        index,
        id_mapping,
        reverse_mapping,
        tombstones,
        next_id: manifest.next_id,
        exact_vectors,
    })
}

fn encode_exact(source: &SnapshotSource<'_>) -> Result<Vec<u8>> {
    let mut rows = Vec::with_capacity(source.id_mapping.len());
    for internal_id in source.id_mapping.values() {
        let vector = source
            .exact_vectors
            .get(*internal_id as usize)
            .and_then(|slot| slot.as_ref())
            .ok_or_else(|| {
                AkiDbError::IndexError(format!(
                    "I8 HNSW index is missing the exact vector for internal id {internal_id}"
                ))
            })?;
        if vector.len() != source.dimensions {
            return Err(AkiDbError::IndexError(format!(
                "exact vector {internal_id} has dimension {}, expected {}",
                vector.len(),
                source.dimensions
            )));
        }
        rows.push((*internal_id, vector.clone()));
    }
    rows.sort_by_key(|(internal_id, _)| *internal_id);
    bincode::serialize(&rows).map_err(|error| AkiDbError::SerializationError(error.to_string()))
}

fn decode_exact(
    bytes: &[u8],
    id_mapping: &HashMap<String, i64>,
    dimensions: usize,
) -> Result<Vec<Option<Vec<f32>>>> {
    let rows: Vec<(i64, Vec<f32>)> = bincode::deserialize(bytes)
        .map_err(|error| AkiDbError::SerializationError(error.to_string()))?;
    if rows.len() != id_mapping.len() {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot exact vector count does not match the graph".to_string(),
        ));
    }
    let mut expected: HashSet<i64> = id_mapping.values().copied().collect();
    let max_id = expected.iter().copied().max().unwrap_or(-1);
    let width = usize::try_from(max_id.saturating_add(1).max(0)).map_err(|_| {
        AkiDbError::IndexError("HNSW snapshot exact vector id does not fit".to_string())
    })?;
    let mut slab = vec![None; width];
    for (internal_id, vector) in rows {
        if internal_id < 0
            || vector.len() != dimensions
            || !expected.remove(&internal_id)
            || slab.get(internal_id as usize).is_none()
        {
            return Err(AkiDbError::IndexError(format!(
                "HNSW snapshot exact vector {internal_id} does not match the id map"
            )));
        }
        slab[internal_id as usize] = Some(vector);
    }
    if !expected.is_empty() {
        return Err(AkiDbError::IndexError(
            "HNSW snapshot is missing an exact vector".to_string(),
        ));
    }
    Ok(slab)
}

fn encode_mappings(
    id_mapping: &HashMap<String, i64>,
    reverse_mapping: &HashMap<i64, VectorId>,
    index: &Index,
    source_next_id: i64,
) -> Result<Vec<u8>> {
    if id_mapping.len() != reverse_mapping.len() || id_mapping.len() != index.size() {
        return Err(AkiDbError::IndexError(
            "HNSW id maps are out of sync with the graph and cannot be snapshotted".to_string(),
        ));
    }
    if let Some(max_id) = id_mapping.values().copied().max() {
        if source_next_id <= max_id {
            return Err(AkiDbError::IndexError(
                "HNSW next id does not cover every mapped vector".to_string(),
            ));
        }
    }
    let mut rows: Vec<(String, i64)> = Vec::with_capacity(id_mapping.len());
    for (external_id, internal_id) in id_mapping {
        if *internal_id < 0 {
            return Err(AkiDbError::IndexError(format!(
                "HNSW mapping for '{external_id}' has a negative internal id"
            )));
        }
        match reverse_mapping.get(internal_id) {
            Some(reverse) if reverse.as_str() == external_id => {}
            _ => {
                return Err(AkiDbError::IndexError(format!(
                    "HNSW forward and reverse maps disagree for '{external_id}'"
                )));
            }
        }
        if !index.contains(*internal_id as u64) {
            return Err(AkiDbError::IndexError(format!(
                "HNSW mapping {internal_id} is missing from the graph"
            )));
        }
        rows.push((external_id.clone(), *internal_id));
    }
    rows.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
    bincode::serialize(&rows).map_err(|error| AkiDbError::SerializationError(error.to_string()))
}

fn precision_name(precision: VectorPrecision) -> &'static str {
    match precision {
        VectorPrecision::F32 => "f32",
        VectorPrecision::F16 => "f16",
        VectorPrecision::I8 => "i8",
    }
}

fn scalar_precision(index: &Index) -> Result<VectorPrecision> {
    let kind = index.scalar_kind();
    if kind == VectorPrecision::F32.to_scalar_kind() {
        Ok(VectorPrecision::F32)
    } else if kind == VectorPrecision::F16.to_scalar_kind() {
        Ok(VectorPrecision::F16)
    } else if kind == VectorPrecision::I8.to_scalar_kind() {
        Ok(VectorPrecision::I8)
    } else {
        Err(AkiDbError::IndexError(
            "HNSW snapshot scalar kind is not f32 or f16".to_string(),
        ))
    }
}

fn metric_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::L2 => "l2",
        DistanceMetric::InnerProduct => "ip",
    }
}

fn read_manifest(path: &Path) -> Result<SnapshotManifest> {
    let bytes = read_file(path)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        AkiDbError::IndexError(format!(
            "HNSW snapshot manifest at {} is invalid: {error}",
            path.display()
        ))
    })
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    reject_symlink(path)?;
    let metadata =
        fs::metadata(path).map_err(|error| io_error("stat HNSW snapshot", path, error))?;
    if !metadata.is_file() {
        return Err(AkiDbError::IndexError(format!(
            "HNSW snapshot expected a file at {}",
            path.display()
        )));
    }
    fs::read(path).map_err(|error| io_error("read HNSW snapshot", path, error))
}

fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file =
        File::create(path).map_err(|error| io_error("create HNSW snapshot", path, error))?;
    file.write_all(bytes)
        .map_err(|error| io_error("write HNSW snapshot", path, error))?;
    Ok(())
}

fn fsync_file(path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| io_error("open HNSW snapshot for fsync", path, error))?;
    file.sync_all()
        .map_err(|error| io_error("fsync HNSW snapshot", path, error))?;
    Ok(())
}

fn fsync_dir(path: &Path) -> Result<()> {
    let directory =
        File::open(path).map_err(|error| io_error("open HNSW snapshot dir", path, error))?;
    directory
        .sync_all()
        .map_err(|error| io_error("fsync HNSW snapshot dir", path, error))?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = read_file(path)?;
    Ok(sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        AkiDbError::IndexError(format!(
            "HNSW snapshot path is not valid Unicode: {}",
            path.display()
        ))
    })
}

fn reject_symlink(path: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(AkiDbError::IndexError(format!(
                "HNSW snapshot refuses a symlink at {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn publish_directory(staged: &Path, destination: &Path) -> Result<()> {
    reject_symlink(destination)?;
    let parent = destination.parent().unwrap_or(Path::new("."));
    let file_name = destination.file_name().ok_or_else(|| {
        AkiDbError::IndexError(format!(
            "HNSW snapshot destination {} has no file name",
            destination.display()
        ))
    })?;
    let backup = parent.join(format!("{}.bak", file_name.to_string_lossy()));
    if backup.exists() {
        fs::remove_dir_all(&backup)
            .map_err(|error| io_error("remove previous HNSW snapshot backup", &backup, error))?;
    }
    if destination.exists() {
        fs::rename(destination, &backup)
            .map_err(|error| io_error("move previous HNSW snapshot aside", destination, error))?;
    }
    if let Err(error) = fs::rename(staged, destination) {
        if backup.exists() {
            let _ = fs::rename(&backup, destination);
        }
        return Err(io_error("publish HNSW snapshot", destination, error));
    }
    if backup.exists() {
        let _ = fs::remove_dir_all(&backup);
    }
    Ok(())
}

fn io_error(operation: &'static str, path: &Path, error: std::io::Error) -> AkiDbError {
    AkiDbError::IndexError(format!("{operation} at {}: {error}", path.display()))
}

struct StagedDir(PathBuf);

impl Drop for StagedDir {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

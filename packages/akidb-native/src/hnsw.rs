//! HNSW (Hierarchical Navigable Small World) graph implementation.
//!
//! Deterministic guarantees:
//!   - Identical queries always produce identical results.
//!   - Tie-breaking: lexicographic by chunk_id when scores are equal.
//!   - Level assignment is deterministic (hash-based, not random).

use std::collections::BinaryHeap;
use std::cmp::Ordering;

use crate::distance;

// ─── Internal types ─────────────────────────────────────────────────────────

/// A candidate in the priority queue.
#[derive(Clone, Copy)]
struct Candidate {
    id: u32,
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.id == other.id
    }
}
impl Eq for Candidate {}

/// Min-heap ordering: smallest distance first, then smallest id.
struct MinCandidate(Candidate);

impl PartialEq for MinCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for MinCandidate {}

impl PartialOrd for MinCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap (BinaryHeap is max-heap by default).
        other.0.distance
            .partial_cmp(&self.0.distance)
            .unwrap_or(Ordering::Equal)
            .then(other.0.id.cmp(&self.0.id))
    }
}

/// Max-heap ordering: largest distance first, then largest id.
struct MaxCandidate(Candidate);

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.distance
            .partial_cmp(&other.0.distance)
            .unwrap_or(Ordering::Equal)
            .then(self.0.id.cmp(&other.0.id))
    }
}

// ─── HNSW Graph ─────────────────────────────────────────────────────────────

/// HNSW node: adjacency lists per layer.
struct HnswNode {
    connections: Vec<Vec<u32>>,
}

/// The core HNSW index.
pub struct HnswGraph {
    metric: String,
    dimension: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    level_multiplier: f64,

    vectors: Vec<Vec<f32>>,
    nodes: Vec<HnswNode>,
    max_level: i32,
    entry_point: i32,
    distance_fn: fn(&[f32], &[f32]) -> f32,
}

impl HnswGraph {
    pub fn new(
        metric: &str,
        dimension: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Self {
        HnswGraph {
            metric: metric.to_string(),
            dimension,
            m,
            ef_construction,
            ef_search,
            level_multiplier: 1.0 / (m as f64).ln(),
            vectors: Vec::new(),
            nodes: Vec::new(),
            max_level: -1,
            entry_point: -1,
            distance_fn: distance::get_distance_fn(metric),
        }
    }

    /// Build the index from a batch of vectors.
    /// For cosine metric, vectors are normalized before insertion.
    pub fn build(&mut self, vectors: &[Vec<f32>]) {
        self.vectors.clear();
        self.nodes.clear();
        self.max_level = -1;
        self.entry_point = -1;

        // Pre-process: normalize for cosine.
        let processed: Vec<Vec<f32>> = if self.metric == "cosine" {
            vectors.iter().map(|v| distance::normalize(v)).collect()
        } else {
            vectors.to_vec()
        };

        for (i, vec) in processed.iter().enumerate() {
            self.insert_node(vec, i as u32);
        }

        self.vectors = processed;
    }

    /// Search for the topK most similar vectors to the query.
    /// Returns (node_id, score) pairs sorted by descending score.
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<(u32, f32)> {
        if self.nodes.is_empty() || top_k == 0 {
            return Vec::new();
        }

        let processed_query = if self.metric == "cosine" {
            distance::normalize(query)
        } else {
            query.to_vec()
        };

        let mut current_node = self.entry_point;

        // Greedy search from top layer down to layer 1.
        for level in (1..=self.max_level).rev() {
            current_node = self.greedy_search(&processed_query, current_node as u32, level as usize) as i32;
        }

        // Ef-search on layer 0.
        let ef = top_k.max(self.ef_search);
        let candidates = self.search_layer(&processed_query, current_node as u32, ef, 0);

        // Take top-K and convert distances to scores.
        let limit = top_k.min(candidates.len());
        let mut results: Vec<(u32, f32)> = candidates[..limit]
            .iter()
            .map(|c| (c.id, distance::distance_to_score(&self.metric, c.distance)))
            .collect();

        // Deterministic sort: descending score, ascending id for ties.
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });

        results
    }

    /// Search with an inline filter predicate (ADR-024: Pre-filter HNSW).
    ///
    /// Candidates that fail the predicate are traversed (neighbors explored)
    /// but excluded from results. This ensures topK results are always
    /// returned when topK matching candidates exist.
    ///
    /// `filter` returns true for node IDs that PASS the filter.
    pub fn search_filtered<F>(&self, query: &[f32], top_k: usize, filter: F) -> Vec<(u32, f32)>
    where
        F: Fn(u32) -> bool,
    {
        if self.nodes.is_empty() || top_k == 0 {
            return Vec::new();
        }

        let processed_query = if self.metric == "cosine" {
            distance::normalize(query)
        } else {
            query.to_vec()
        };

        let mut current_node = self.entry_point;

        // Greedy search from top layer down to layer 1 (no filtering on upper layers).
        for level in (1..=self.max_level).rev() {
            current_node =
                self.greedy_search(&processed_query, current_node as u32, level as usize) as i32;
        }

        // Filtered ef-search on layer 0 with expanded exploration.
        let ef = (top_k * 3).max(self.ef_search);
        let candidates =
            self.search_layer_filtered(&processed_query, current_node as u32, ef, 0, &filter);

        // If graph traversal didn't yield enough results, brute-force fallback.
        let mut results = if candidates.len() < top_k {
            self.brute_force_filtered(&processed_query, top_k, &filter)
        } else {
            candidates
        };

        // Take top-K and convert distances to scores.
        results.truncate(top_k);
        let mut scored: Vec<(u32, f32)> = results
            .iter()
            .map(|c| (c.id, distance::distance_to_score(&self.metric, c.distance)))
            .collect();

        // Deterministic sort: descending score, ascending id for ties.
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });

        scored
    }

    /// Serialize to the binary format (v2), compatible with HnswIndex.
    pub fn serialize(&self) -> Vec<u8> {
        let metric_bytes = self.metric.as_bytes();
        let node_count = self.nodes.len();
        let vector_count = self.vectors.len();

        // Calculate graph block size.
        let mut graph_size = 0usize;
        for node in &self.nodes {
            graph_size += 4; // layer_count
            for layer in &node.connections {
                graph_size += 4 + layer.len() * 4; // neighbor_count + neighbors
            }
        }

        let header_size = 48 + metric_bytes.len();
        let vectors_size = vector_count * self.dimension * 4;
        let total_size = header_size + vectors_size + graph_size;

        let mut buf = vec![0u8; total_size];
        let mut offset = 0;

        // Magic "HNSW".
        buf[offset..offset + 4].copy_from_slice(b"HNSW");
        offset += 4;

        // Version = 2.
        write_u32_le(&mut buf, offset, 2);
        offset += 4;

        // M.
        write_u32_le(&mut buf, offset, self.m as u32);
        offset += 4;

        // efConstruction.
        write_u32_le(&mut buf, offset, self.ef_construction as u32);
        offset += 4;

        // efSearch.
        write_u32_le(&mut buf, offset, self.ef_search as u32);
        offset += 4;

        // Metric (length-prefixed).
        write_u32_le(&mut buf, offset, metric_bytes.len() as u32);
        offset += 4;
        buf[offset..offset + metric_bytes.len()].copy_from_slice(metric_bytes);
        offset += metric_bytes.len();

        // Dimension.
        write_u32_le(&mut buf, offset, self.dimension as u32);
        offset += 4;

        // maxLevel (signed).
        write_i32_le(&mut buf, offset, self.max_level);
        offset += 4;

        // entryPoint (signed).
        write_i32_le(&mut buf, offset, self.entry_point);
        offset += 4;

        // nodeCount.
        write_u32_le(&mut buf, offset, node_count as u32);
        offset += 4;

        // vectorCount.
        write_u32_le(&mut buf, offset, vector_count as u32);
        offset += 4;

        // Vectors block.
        for vec in &self.vectors {
            for &val in vec {
                write_f32_le(&mut buf, offset, val);
                offset += 4;
            }
        }

        // Graph block.
        for node in &self.nodes {
            let layer_count = node.connections.len();
            write_u32_le(&mut buf, offset, layer_count as u32);
            offset += 4;

            for layer in &node.connections {
                write_u32_le(&mut buf, offset, layer.len() as u32);
                offset += 4;
                for &neighbor_id in layer {
                    write_u32_le(&mut buf, offset, neighbor_id);
                    offset += 4;
                }
            }
        }

        buf
    }

    /// Deserialize from the binary format (v2).
    pub fn deserialize(&mut self, data: &[u8]) -> Result<(), String> {
        let len = data.len();

        // Minimum header: magic(4) + version(4) + M(4) + efC(4) + efS(4) + metric_len(4) = 24
        if len < 24 {
            return Err("Buffer too small for HNSW header".into());
        }

        // Check magic.
        if &data[0..4] != b"HNSW" {
            return Err("Invalid magic bytes".into());
        }

        let mut offset = 4;

        /// Helper macro: check that `offset + n` bytes are available before reading.
        macro_rules! check_bounds {
            ($n:expr, $label:expr) => {
                if offset + $n > len {
                    return Err(format!(
                        "Truncated HNSW data: need {} bytes for {} at offset {}, but only {} remain",
                        $n, $label, offset, len - offset
                    ));
                }
            };
        }

        check_bounds!(4, "version");
        let version = read_u32_le(data, offset);
        offset += 4;
        if version != 2 {
            return Err(format!("Unsupported version: {version}"));
        }

        check_bounds!(12, "M/efConstruction/efSearch");
        let _m = read_u32_le(data, offset) as usize;
        offset += 4;
        let _ef_construction = read_u32_le(data, offset) as usize;
        offset += 4;
        let _ef_search = read_u32_le(data, offset) as usize;
        offset += 4;

        // Metric.
        check_bounds!(4, "metric_len");
        let metric_len = read_u32_le(data, offset) as usize;
        offset += 4;
        check_bounds!(metric_len, "metric string");
        let metric = std::str::from_utf8(&data[offset..offset + metric_len])
            .map_err(|e| format!("Invalid metric string: {e}"))?;
        offset += metric_len;

        if metric != self.metric {
            return Err(format!(
                "Metric mismatch: index has {metric}, expected {}",
                self.metric
            ));
        }

        check_bounds!(4, "dimension");
        let dimension = read_u32_le(data, offset) as usize;
        offset += 4;
        if dimension != self.dimension {
            return Err(format!(
                "Dimension mismatch: index has {dimension}, expected {}",
                self.dimension
            ));
        }

        check_bounds!(16, "max_level/entry_point/node_count/vector_count");
        let max_level = read_i32_le(data, offset);
        offset += 4;
        let entry_point = read_i32_le(data, offset);
        offset += 4;
        let node_count = read_u32_le(data, offset) as usize;
        offset += 4;
        let vector_count = read_u32_le(data, offset) as usize;
        offset += 4;

        // Read vectors.
        let mut vectors = Vec::with_capacity(vector_count);
        for i in 0..vector_count {
            let needed = dimension * 4;
            check_bounds!(needed, &format!("vector {i}"));
            let mut vec = vec![0.0_f32; dimension];
            for v in vec.iter_mut() {
                *v = read_f32_le(data, offset);
                offset += 4;
            }
            vectors.push(vec);
        }

        // Read graph.
        let mut nodes = Vec::with_capacity(node_count);
        for i in 0..node_count {
            check_bounds!(4, &format!("node {i} layer_count"));
            let layer_count = read_u32_le(data, offset) as usize;
            offset += 4;
            let mut connections = Vec::with_capacity(layer_count);

            for l in 0..layer_count {
                check_bounds!(4, &format!("node {i} layer {l} neighbor_count"));
                let neighbor_count = read_u32_le(data, offset) as usize;
                offset += 4;
                let needed = neighbor_count * 4;
                check_bounds!(needed, &format!("node {i} layer {l} neighbors"));
                let mut neighbors = Vec::with_capacity(neighbor_count);
                for _ in 0..neighbor_count {
                    neighbors.push(read_u32_le(data, offset));
                    offset += 4;
                }
                neighbors.sort_unstable();
                connections.push(neighbors);
            }

            nodes.push(HnswNode { connections });
        }

        self.max_level = max_level;
        self.entry_point = entry_point;
        self.vectors = vectors;
        self.nodes = nodes;

        Ok(())
    }

    // ─── Internal: level assignment ─────────────────────────────────────────

    /// Deterministic level assignment matching the TypeScript implementation.
    fn assign_level(&self, node_id: u32) -> usize {
        let hash = ((node_id as f64 + 1.0).sin() * 2147483647.0).abs();
        let uniform = (hash % 1000000.0) / 1000000.0;
        let clamped = uniform.max(1e-10);
        (-clamped.ln() * self.level_multiplier).floor() as usize
    }

    // ─── Internal: insertion ────────────────────────────────────────────────

    fn insert_node(&mut self, vector: &[f32], node_id: u32) {
        let level = self.assign_level(node_id);

        let node = HnswNode {
            connections: (0..=level).map(|_| Vec::new()).collect(),
        };
        self.nodes.push(node);

        // Store vector during construction.
        let id = node_id as usize;
        while self.vectors.len() <= id {
            self.vectors.push(Vec::new());
        }
        self.vectors[id] = vector.to_vec();

        // First node — make it the entry point.
        if self.entry_point == -1 {
            self.entry_point = node_id as i32;
            self.max_level = level as i32;
            return;
        }

        let mut current_node = self.entry_point as u32;

        // Greedy descent from top layer down to (level + 1).
        for lc in ((level as i32 + 1)..=self.max_level).rev() {
            current_node = self.greedy_search(vector, current_node, lc as usize);
        }

        // Insert into layers [min(level, max_level) .. 0].
        let insert_up_to = level.min(self.max_level as usize);
        for lc in (0..=insert_up_to).rev() {
            let neighbors = self.search_layer(vector, current_node, self.ef_construction, lc);

            let max_connections = if lc == 0 { self.m * 2 } else { self.m };
            let selected: Vec<Candidate> = neighbors
                .into_iter()
                .take(max_connections)
                .collect();

            // Set connections for the new node.
            let node_idx = node_id as usize;
            self.ensure_layer_exists(node_idx, lc);
            let mut selected_ids: Vec<u32> = selected.iter().map(|c| c.id).collect();
            selected_ids.sort_unstable();
            self.nodes[node_idx].connections[lc] = selected_ids.clone();

            // Bidirectional connections.
            for &neighbor_id in &selected_ids {
                self.add_connection(neighbor_id, node_id, lc, max_connections);
            }

            // Use closest neighbor as entry for next layer down.
            if !selected_ids.is_empty() {
                current_node = selected_ids[0];
            }
        }

        // Update entry point if this node has a higher level.
        if level as i32 > self.max_level {
            self.entry_point = node_id as i32;
            self.max_level = level as i32;
        }
    }

    fn ensure_layer_exists(&mut self, node_idx: usize, layer: usize) {
        while self.nodes[node_idx].connections.len() <= layer {
            self.nodes[node_idx].connections.push(Vec::new());
        }
    }

    fn add_connection(&mut self, from_id: u32, to_id: u32, layer: usize, max_connections: usize) {
        let from_idx = from_id as usize;
        if from_idx >= self.nodes.len() {
            return;
        }

        self.ensure_layer_exists(from_idx, layer);

        let conns = &mut self.nodes[from_idx].connections[layer];
        match conns.binary_search(&to_id) {
            Ok(_) => return,
            Err(pos) => conns.insert(pos, to_id),
        }

        if conns.len() > max_connections {
            self.prune_connections(from_idx, layer, max_connections);
        }
    }

    fn prune_connections(&mut self, node_idx: usize, layer: usize, max_connections: usize) {
        let node_vector = self.vectors[node_idx].clone();
        let conns = self.nodes[node_idx].connections[layer].clone();

        let mut with_distance: Vec<Candidate> = conns
            .iter()
            .map(|&neighbor_id| Candidate {
                id: neighbor_id,
                distance: (self.distance_fn)(&node_vector, &self.vectors[neighbor_id as usize]),
            })
            .collect();

        with_distance.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });

        let mut pruned: Vec<u32> = with_distance.iter().take(max_connections).map(|c| c.id).collect();
        pruned.sort_unstable();
        self.nodes[node_idx].connections[layer] = pruned;
    }

    // ─── Internal: search ───────────────────────────────────────────────────

    fn greedy_search(&self, query: &[f32], entry_id: u32, layer: usize) -> u32 {
        let mut current_id = entry_id;
        let mut current_dist = (self.distance_fn)(query, &self.vectors[current_id as usize]);

        loop {
            let node = &self.nodes[current_id as usize];
            if layer >= node.connections.len() {
                break;
            }

            let neighbors = &node.connections[layer];

            let mut improved = false;
            for &neighbor_id in neighbors {
                let dist = (self.distance_fn)(query, &self.vectors[neighbor_id as usize]);
                if dist < current_dist || (dist == current_dist && neighbor_id < current_id) {
                    current_id = neighbor_id;
                    current_dist = dist;
                    improved = true;
                }
            }

            if !improved {
                break;
            }
        }

        current_id
    }

    /// Layer-0 beam search with inline filter.
    /// Non-matching candidates are traversed (neighbors explored) but excluded
    /// from the result set, preserving graph connectivity.
    fn search_layer_filtered<F>(
        &self,
        query: &[f32],
        entry_id: u32,
        ef: usize,
        layer: usize,
        filter: &F,
    ) -> Vec<Candidate>
    where
        F: Fn(u32) -> bool,
    {
        let entry_dist = (self.distance_fn)(query, &self.vectors[entry_id as usize]);
        let entry_candidate = Candidate {
            id: entry_id,
            distance: entry_dist,
        };

        // candidates: min-heap (closest first — next to explore).
        let mut candidates: BinaryHeap<MinCandidate> = BinaryHeap::new();
        candidates.push(MinCandidate(entry_candidate));

        // All explored candidates (for termination condition).
        let mut all_results: BinaryHeap<MaxCandidate> = BinaryHeap::new();
        all_results.push(MaxCandidate(entry_candidate));

        // Filtered results only (candidates that pass the filter), bounded at ef.
        // Use a max-heap so we can evict the worst when over capacity.
        let mut filtered_heap: BinaryHeap<MaxCandidate> = BinaryHeap::new();
        if filter(entry_id) {
            filtered_heap.push(MaxCandidate(entry_candidate));
        }

        let mut visited = std::collections::HashSet::new();
        visited.insert(entry_id);

        while let Some(MinCandidate(current)) = candidates.pop() {
            // Stop when the closest unexplored candidate is farther than the worst
            // in our exploration set (standard HNSW termination).
            let worst_all = all_results.peek().unwrap();
            if current.distance > worst_all.0.distance && all_results.len() >= ef {
                break;
            }

            let node = &self.nodes[current.id as usize];
            if layer >= node.connections.len() {
                continue;
            }

            let neighbors = &node.connections[layer];

            for &neighbor_id in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);

                let dist = (self.distance_fn)(query, &self.vectors[neighbor_id as usize]);
                let c = Candidate {
                    id: neighbor_id,
                    distance: dist,
                };

                // Always add to exploration set (preserves graph connectivity).
                let current_worst = all_results.peek().unwrap();
                if all_results.len() < ef || dist < current_worst.0.distance {
                    candidates.push(MinCandidate(c));
                    all_results.push(MaxCandidate(c));
                    if all_results.len() > ef {
                        all_results.pop();
                    }
                }

                // Only add to filtered results if it passes the filter, bounded at ef.
                if filter(neighbor_id) {
                    filtered_heap.push(MaxCandidate(c));
                    if filtered_heap.len() > ef {
                        filtered_heap.pop();
                    }
                }
            }
        }

        // Convert bounded heap to sorted vec: ascending distance, ascending id for ties.
        let mut filtered_results: Vec<Candidate> = filtered_heap.into_iter().map(|mc| mc.0).collect();
        filtered_results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });

        filtered_results
    }

    /// Brute-force fallback for extremely selective filters.
    /// Scans all vectors, applies the filter, returns top-K matches.
    fn brute_force_filtered<F>(&self, query: &[f32], top_k: usize, filter: &F) -> Vec<Candidate>
    where
        F: Fn(u32) -> bool,
    {
        let mut results: Vec<Candidate> = Vec::new();
        for (i, vec) in self.vectors.iter().enumerate() {
            let id = i as u32;
            if !filter(id) || vec.is_empty() {
                continue;
            }
            let dist = (self.distance_fn)(query, vec);
            results.push(Candidate { id, distance: dist });
        }

        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });
        results.truncate(top_k);
        results
    }

    fn search_layer(&self, query: &[f32], entry_id: u32, ef: usize, layer: usize) -> Vec<Candidate> {
        let entry_dist = (self.distance_fn)(query, &self.vectors[entry_id as usize]);
        let entry_candidate = Candidate {
            id: entry_id,
            distance: entry_dist,
        };

        // candidates: min-heap (closest first — next to explore).
        let mut candidates: BinaryHeap<MinCandidate> = BinaryHeap::new();
        candidates.push(MinCandidate(entry_candidate));

        // results: max-heap (farthest first — for pruning).
        let mut results: BinaryHeap<MaxCandidate> = BinaryHeap::new();
        results.push(MaxCandidate(entry_candidate));

        let mut visited = std::collections::HashSet::new();
        visited.insert(entry_id);

        while let Some(MinCandidate(current)) = candidates.pop() {
            let worst_result = results.peek().unwrap();
            if current.distance > worst_result.0.distance {
                break;
            }

            let node = &self.nodes[current.id as usize];
            if layer >= node.connections.len() {
                continue;
            }

            let neighbors = &node.connections[layer];

            for &neighbor_id in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);

                let dist = (self.distance_fn)(query, &self.vectors[neighbor_id as usize]);
                let current_worst = results.peek().unwrap();

                if results.len() < ef || dist < current_worst.0.distance {
                    let c = Candidate {
                        id: neighbor_id,
                        distance: dist,
                    };
                    candidates.push(MinCandidate(c));
                    results.push(MaxCandidate(c));

                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        // Extract and sort ascending by distance.
        let mut output: Vec<Candidate> = results
            .into_iter()
            .map(|MaxCandidate(c)| c)
            .collect();

        output.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then(a.id.cmp(&b.id))
        });

        output
    }
}

// ─── Binary I/O helpers ─────────────────────────────────────────────────────

#[inline]
fn write_u32_le(buf: &mut [u8], offset: usize, val: u32) {
    buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
fn write_i32_le(buf: &mut [u8], offset: usize, val: i32) {
    buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
fn write_f32_le(buf: &mut [u8], offset: usize, val: f32) {
    buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline]
fn read_u32_le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

#[inline]
fn read_i32_le(data: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

#[inline]
fn read_f32_le(data: &[u8], offset: usize) -> f32 {
    f32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec4(a: f32, b: f32, c: f32, d: f32) -> Vec<f32> {
        vec![a, b, c, d]
    }

    #[test]
    fn test_build_and_search_cosine() {
        let mut graph = HnswGraph::new("cosine", 4, 16, 200, 100);
        let vectors = vec![
            vec4(1.0, 0.0, 0.0, 0.0),
            vec4(0.0, 1.0, 0.0, 0.0),
            vec4(0.0, 0.0, 1.0, 0.0),
            vec4(0.0, 0.0, 0.0, 1.0),
        ];

        graph.build(&vectors);
        let results = graph.search(&[1.0, 0.0, 0.0, 0.0], 1);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0); // node 0 = [1,0,0,0]
        assert!((results[0].1 - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_build_and_search_l2() {
        let mut graph = HnswGraph::new("l2", 4, 16, 200, 100);
        let vectors = vec![
            vec4(1.0, 0.0, 0.0, 0.0),
            vec4(0.0, 1.0, 0.0, 0.0),
            vec4(0.9, 0.1, 0.0, 0.0),
        ];

        graph.build(&vectors);
        let results = graph.search(&[1.0, 0.0, 0.0, 0.0], 1);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_top_k_sorted() {
        let mut graph = HnswGraph::new("cosine", 4, 16, 200, 100);
        let vectors = vec![
            vec4(1.0, 0.0, 0.0, 0.0),
            vec4(0.9, 0.1, 0.0, 0.0),
            vec4(0.5, 0.5, 0.0, 0.0),
            vec4(0.0, 1.0, 0.0, 0.0),
        ];

        graph.build(&vectors);
        let results = graph.search(&[1.0, 0.0, 0.0, 0.0], 3);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0); // exact match
        for i in 1..results.len() {
            assert!(results[i - 1].1 >= results[i].1);
        }
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut graph = HnswGraph::new("cosine", 4, 16, 200, 100);
        let vectors = vec![
            vec4(1.0, 0.0, 0.0, 0.0),
            vec4(0.0, 1.0, 0.0, 0.0),
            vec4(0.7, 0.7, 0.0, 0.0),
        ];

        graph.build(&vectors);
        let query = vec![0.8, 0.2, 0.0, 0.0];
        let before = graph.search(&query, 3);

        let serialized = graph.serialize();
        let mut restored = HnswGraph::new("cosine", 4, 16, 200, 100);
        restored.deserialize(&serialized).unwrap();

        let after = restored.search(&query, 3);
        assert_eq!(before.len(), after.len());
        for i in 0..before.len() {
            assert_eq!(before[i].0, after[i].0);
            assert!((before[i].1 - after[i].1).abs() < 1e-4);
        }
    }

    #[test]
    fn test_deterministic() {
        let mut graph = HnswGraph::new("cosine", 4, 16, 200, 100);
        let vectors: Vec<Vec<f32>> = (0..50)
            .map(|i| {
                let i = i as f32;
                vec![
                    (i * 0.7).sin(),
                    (i * 0.3).cos(),
                    (i * 1.1).sin(),
                    (i * 0.5).cos(),
                ]
            })
            .collect();

        graph.build(&vectors);
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let r1 = graph.search(&query, 10);
        let r2 = graph.search(&query, 10);
        let r3 = graph.search(&query, 10);

        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    // ── search_filtered tests ────────────────────────────────────────────

    #[test]
    fn test_search_filtered_returns_topk_with_selective_filter() {
        // 100 vectors, filter matches only even node IDs (50 match).
        // Request topK=10 — should get exactly 10 results.
        let mut graph = HnswGraph::new("l2", 4, 16, 200, 100);
        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| {
                let f = i as f32;
                vec![f.sin(), f.cos(), (f * 0.5).sin(), (f * 0.3).cos()]
            })
            .collect();

        graph.build(&vectors);
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let results = graph.search_filtered(&query, 10, |id| id % 2 == 0);

        assert_eq!(results.len(), 10);
        // All returned IDs must pass the filter.
        for (id, _score) in &results {
            assert_eq!(id % 2, 0, "node {} should be even", id);
        }
        // Scores must be descending.
        for i in 1..results.len() {
            assert!(results[i - 1].1 >= results[i].1);
        }
    }

    #[test]
    fn test_search_filtered_returns_all_when_fewer_than_topk() {
        // 20 vectors, filter matches only 3 (nodes 0, 5, 10).
        // Request topK=10 — should get exactly 3 results.
        let mut graph = HnswGraph::new("l2", 4, 16, 200, 100);
        let vectors: Vec<Vec<f32>> = (0..20)
            .map(|i| {
                let f = i as f32;
                vec![f.sin(), f.cos(), (f * 0.5).sin(), (f * 0.3).cos()]
            })
            .collect();

        graph.build(&vectors);
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let allowed: std::collections::HashSet<u32> = [0, 5, 10].iter().copied().collect();
        let results = graph.search_filtered(&query, 10, |id| allowed.contains(&id));

        assert_eq!(results.len(), 3);
        for (id, _score) in &results {
            assert!(allowed.contains(id), "node {} should be in allowed set", id);
        }
    }

    #[test]
    fn test_search_filtered_no_filter_matches_unfiltered() {
        // With a pass-all filter, results should match unfiltered search().
        let mut graph = HnswGraph::new("cosine", 4, 16, 200, 100);
        let vectors: Vec<Vec<f32>> = (0..30)
            .map(|i| {
                let f = i as f32;
                vec![
                    (f * 0.7).sin(),
                    (f * 0.3).cos(),
                    (f * 1.1).sin(),
                    (f * 0.5).cos(),
                ]
            })
            .collect();

        graph.build(&vectors);
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let unfiltered = graph.search(&query, 5);
        let filtered = graph.search_filtered(&query, 5, |_| true);

        assert_eq!(unfiltered.len(), filtered.len());
        for i in 0..unfiltered.len() {
            assert_eq!(unfiltered[i].0, filtered[i].0);
            assert!((unfiltered[i].1 - filtered[i].1).abs() < 1e-4);
        }
    }

    #[test]
    fn test_search_filtered_deterministic() {
        let mut graph = HnswGraph::new("cosine", 4, 16, 200, 100);
        let vectors: Vec<Vec<f32>> = (0..50)
            .map(|i| {
                let f = i as f32;
                vec![
                    (f * 0.7).sin(),
                    (f * 0.3).cos(),
                    (f * 1.1).sin(),
                    (f * 0.5).cos(),
                ]
            })
            .collect();

        graph.build(&vectors);
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let filter = |id: u32| id.is_multiple_of(3);
        let r1 = graph.search_filtered(&query, 10, filter);
        let r2 = graph.search_filtered(&query, 10, filter);
        let r3 = graph.search_filtered(&query, 10, filter);

        assert_eq!(r1.len(), r2.len());
        assert_eq!(r2.len(), r3.len());
        for i in 0..r1.len() {
            assert_eq!(r1[i].0, r2[i].0);
            assert_eq!(r2[i].0, r3[i].0);
            assert!((r1[i].1 - r2[i].1).abs() < 1e-6);
            assert!((r2[i].1 - r3[i].1).abs() < 1e-6);
        }
    }
}

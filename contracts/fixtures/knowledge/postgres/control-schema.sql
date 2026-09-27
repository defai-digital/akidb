-- Minimal disposable qualification schema; not a production migration.
create table knowledge_schema_migrations (
  version integer primary key,
  name text not null
);
insert into knowledge_schema_migrations(version, name)
values (1, 'authoritative_knowledge_control_plane');

create table knowledge_streams (
  workspace_id text not null,
  collection text not null,
  next_sequence bigint not null,
  active_generation_id text,
  active_manifest_sha256 char(64),
  active_target_sequence bigint not null,
  publication_generation_id text,
  stream_version bigint not null,
  minimum_ready_replicas smallint not null,
  minimum_failure_domains smallint not null,
  heartbeat_ttl_ms integer not null,
  updated_at timestamptz not null default clock_timestamp(),
  primary key (workspace_id, collection)
);

create table knowledge_generations (
  generation_id text primary key,
  workspace_id text not null,
  collection text not null,
  status text not null,
  manifest jsonb not null,
  manifest_bytes bytea not null,
  manifest_sha256 char(64) not null,
  bundle_uri text not null,
  bundle_sha256 char(64) not null,
  required_sequence bigint not null,
  materialization_digest char(64),
  materialized_vector_count bigint,
  materialized_edge_count bigint
);

create table knowledge_mutations (
  workspace_id text not null,
  collection text not null,
  sequence bigint not null,
  mutation_id text not null unique,
  generation_id text not null,
  contract jsonb not null,
  primary key (workspace_id, collection, sequence)
);

create table knowledge_replicas (
  replica_id text primary key,
  endpoint text not null,
  failure_domain text not null,
  software_version text not null,
  index_format_version text not null,
  supported_knowledge_schema_versions jsonb not null,
  supported_graph_schema_versions jsonb not null,
  process_ready boolean not null,
  drained boolean not null,
  heartbeat_at timestamptz not null,
  registered_at timestamptz not null default clock_timestamp(),
  updated_at timestamptz not null
);

create table knowledge_replica_checkpoints (
  replica_id text not null,
  workspace_id text not null,
  collection text not null,
  generation_id text not null,
  manifest_sha256 char(64) not null,
  applied_sequence bigint not null,
  state text not null,
  last_error text,
  vector_count bigint not null,
  edge_count bigint not null,
  generation_digest char(64) not null,
  index_ready boolean not null,
  updated_at timestamptz not null,
  primary key (replica_id, workspace_id, collection, generation_id)
);

create function knowledge_reconcile_generation_ready(text)
returns boolean language sql as $$ select true $$;

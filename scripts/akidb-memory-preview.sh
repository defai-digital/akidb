#!/usr/bin/env bash
# Start the local authoritative Memory developer preview without cloud or
# embedding dependencies. This is an experimental single-workspace profile.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PREVIEW_DATA_DIR="$PROJECT_ROOT/data/memory-preview"
PRINCIPAL_TOKEN_FILE="$PREVIEW_DATA_DIR/principal.token"
LEGACY_TOKEN_FILE="$PREVIEW_DATA_DIR/legacy.token"
CONFIG_FILE="$PROJECT_ROOT/config/memory-preview.toml"
LISTEN_ADDRESS="${AKIDB_MEMORY_PREVIEW_LISTEN:-127.0.0.1:50051}"
MODE="server"

if [[ "${1:-}" == "--mcp" ]]; then
  MODE="mcp"
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--mcp]" >&2
  exit 2
fi

if [[ "$MODE" == "server" ]]; then
  if [[ "$LISTEN_ADDRESS" =~ ^127\.0\.0\.1:([1-9][0-9]{0,4})$ \
    || "$LISTEN_ADDRESS" =~ ^\[::1\]:([1-9][0-9]{0,4})$ ]]; then
    LISTEN_PORT="${BASH_REMATCH[1]}"
  else
    echo "developer preview listener must use 127.0.0.1 or [::1]" >&2
    exit 2
  fi
  if [[ "$LISTEN_PORT" -lt 1 || "$LISTEN_PORT" -gt 65535 ]]; then
    echo "developer preview listener port must be from 1 through 65535" >&2
    exit 2
  fi
fi

if [[ -L "$PREVIEW_DATA_DIR" \
  || ( -e "$PREVIEW_DATA_DIR" && ! -d "$PREVIEW_DATA_DIR" ) ]]; then
  echo "preview data path must be a real directory, not a symlink" >&2
  exit 1
fi
mkdir -p "$PREVIEW_DATA_DIR"
chmod 700 "$PREVIEW_DATA_DIR"

create_token_file() {
  local token_file="$1"
  local token
  if [[ -e "$token_file" || -L "$token_file" ]]; then
    if [[ -L "$token_file" || ! -f "$token_file" || ! -s "$token_file" ]]; then
      echo "existing token path must be a non-empty regular file: $token_file" >&2
      exit 1
    fi
    chmod 600 "$token_file"
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    token="$(openssl rand -hex 32)"
  else
    token="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
  fi
  if ! (
    umask 077
    set -o noclobber
    printf '%s\n' "$token" >"$token_file"
  ); then
    echo "refusing to overwrite token path created concurrently: $token_file" >&2
    exit 1
  fi
  chmod 600 "$token_file"
}

create_token_file "$PRINCIPAL_TOKEN_FILE"
create_token_file "$LEGACY_TOKEN_FILE"

cd "$PROJECT_ROOT"
echo "AkiDB authoritative Memory DEVELOPER PREVIEW"
echo "Profile: experimental, local, one authoritative workspace per process"
echo "Temporal/history APIs: implemented for evaluation; not production-qualified"
echo "Credential file: $PRINCIPAL_TOKEN_FILE (mode 0600; token not printed)"

if [[ "$MODE" == "mcp" ]]; then
  export AKIDB_MCP_AUTH_TOKEN
  AKIDB_MCP_AUTH_TOKEN="$(<"$PRINCIPAL_TOKEN_FILE")"
  export AKIDB_MCP_MEMORY_NAMESPACE="memory/default"
  export AKIDB_MCP_MEMORY_PURPOSE="agent-memory"
  export AKIDB_MCP_MEMORY_AGENT="agent:local-preview"
  exec cargo run -p akidb-cli -- mcp --standalone --config "$CONFIG_FILE"
fi

exec cargo run -p akidb-cli -- server \
  --standalone \
  --config "$CONFIG_FILE" \
  --listen "$LISTEN_ADDRESS"

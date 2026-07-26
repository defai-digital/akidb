#!/usr/bin/env bash
set -euo pipefail

if (( $# < 2 )); then
  echo "usage: $0 OUTPUT_DIR HOST=IP [HOST=IP ...]" >&2
  exit 1
fi

output_dir="$1"
shift

if [[ "$output_dir" != /* ]]; then
  echo "OUTPUT_DIR must be an absolute, dedicated PKI directory" >&2
  exit 1
fi

umask 077
mkdir -p "$output_dir/hosts"

ca_key="$output_dir/ca.key"
ca_cert="$output_dir/ca.crt"
if [[ -e "$ca_key" || -e "$ca_cert" ]]; then
  if [[ ! -s "$ca_key" || ! -s "$ca_cert" ]]; then
    echo "refusing to overwrite a partial CA in $output_dir" >&2
    exit 1
  fi
else
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out "$ca_key"
  openssl req \
    -x509 \
    -new \
    -sha256 \
    -days 3650 \
    -key "$ca_key" \
    -subj "/CN=AkiDB Knowledge Cell Lab CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash" \
    -out "$ca_cert"
fi

if ! openssl x509 -in "$ca_cert" -noout -ext basicConstraints \
    | grep -q "CA:TRUE" \
  || ! openssl x509 -in "$ca_cert" -noout -ext keyUsage \
    | grep -q "Certificate Sign"; then
  echo "CA certificate must contain CA:TRUE and keyCertSign extensions" >&2
  exit 1
fi

for mapping in "$@"; do
  name="${mapping%%=*}"
  address="${mapping#*=}"
  if [[ "$name" == "$mapping" \
      || ! "$name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$ \
      || ! "$address" =~ ^[0-9A-Fa-f:.]+$ ]]; then
    echo "invalid HOST=IP mapping: $mapping" >&2
    exit 1
  fi

  key="$output_dir/hosts/$name.key"
  cert="$output_dir/hosts/$name.crt"
  if [[ -e "$key" || -e "$cert" ]]; then
    if [[ ! -s "$key" || ! -s "$cert" ]]; then
      echo "refusing to overwrite partial credentials for $name" >&2
      exit 1
    fi
    continue
  fi

  request="$(mktemp "${TMPDIR:-/tmp}/akidb-pki-request.XXXXXXXX")"
  extensions="$(mktemp "${TMPDIR:-/tmp}/akidb-pki-extensions.XXXXXXXX")"
  trap 'find "$request" "$extensions" -delete 2>/dev/null || true' EXIT
  printf '%s\n' \
    "basicConstraints=critical,CA:FALSE" \
    "subjectAltName=DNS:$name,IP:$address" \
    "extendedKeyUsage=serverAuth" \
    "keyUsage=digitalSignature,keyEncipherment" \
    "subjectKeyIdentifier=hash" \
    "authorityKeyIdentifier=keyid,issuer" \
    >"$extensions"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$key"
  openssl req \
    -new \
    -sha256 \
    -key "$key" \
    -subj "/CN=$name" \
    -out "$request"
  openssl x509 \
    -req \
    -sha256 \
    -days 825 \
    -in "$request" \
    -CA "$ca_cert" \
    -CAkey "$ca_key" \
    -CAcreateserial \
    -extfile "$extensions" \
    -out "$cert"
  find "$request" "$extensions" -delete
  trap - EXIT
done

chmod 0600 "$ca_key" "$output_dir"/hosts/*.key
chmod 0644 "$ca_cert" "$output_dir"/hosts/*.crt
openssl verify -purpose sslserver -CAfile "$ca_cert" "$output_dir"/hosts/*.crt
printf 'PKI directory: %s\n' "$output_dir"

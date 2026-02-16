#!/usr/bin/env sh
set -eu

ENDPOINT="${SPICEDB_ENDPOINT:-spicedb:50051}"
TOKEN="${SPICEDB_TOKEN:-testtoken}"
SCHEMA_PATH="${SCHEMA_PATH:-/workspace/examples/spicedb-schema.zed}"
REL_PATH="${RELATIONSHIPS_PATH:-/workspace/examples/relationships.zed}"

echo "waiting for SpiceDB at ${ENDPOINT}..."
i=0
until zed schema write "$SCHEMA_PATH" --endpoint "$ENDPOINT" --token "$TOKEN" --insecure >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "timed out waiting for SpiceDB"
    exit 1
  fi
  sleep 1
done

echo "schema loaded; importing relationships..."
while IFS= read -r line; do
  case "$line" in
    ""|\#*) continue ;;
  esac
  zed relationship create "$line" --endpoint "$ENDPOINT" --token "$TOKEN" --insecure >/dev/null
done < "$REL_PATH"

echo "spicedb seed complete"

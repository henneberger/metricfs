#!/usr/bin/env sh
set -eu

if [ "${1:-}" = "metricfs" ]; then
  # Provide a mountpoint if caller passed env vars.
  if [ "${METRICFS_MOUNT_DIR:-}" != "" ]; then
    mkdir -p "${METRICFS_MOUNT_DIR}"
  fi
fi

exec "$@"

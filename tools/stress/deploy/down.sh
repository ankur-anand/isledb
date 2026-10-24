#!/usr/bin/env bash
# down.sh stops SILO, keeping its data under STRESS_DATA.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
STRESS_DATA=${STRESS_DATA:-/nonexistent} docker compose -f "$HERE/compose.yaml" down

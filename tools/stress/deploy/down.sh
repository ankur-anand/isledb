#!/usr/bin/env bash
# down.sh stops SILO, Prometheus and Grafana, keeping their data under
# STRESS_DATA.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
STRESS_DATA=${STRESS_DATA:-/nonexistent} STRESS_RESULTS=${STRESS_RESULTS:-/nonexistent} \
    docker compose -f "$HERE/compose.yaml" --profile monitoring down

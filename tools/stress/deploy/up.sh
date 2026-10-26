#!/usr/bin/env bash
# up.sh starts SILO, creates the isledb bucket with a SigV4-signed PUT so no
# S3 client is needed, and starts Prometheus and Grafana; MONITORING=0 leaves
# those two out. down.sh stops everything. All of it listens on 127.0.0.1
# only.
#
#   STRESS_DATA=/data/isledb-stress SILO_CPUS=6 ./up.sh
#
# Run the stress test with -out "$STRESS_RESULTS" (by default
# $STRESS_DATA/results), where Prometheus finds the runs to scrape.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
: "${STRESS_DATA:?set STRESS_DATA to a directory on the disk under test}"
: "${STRESS_RESULTS:=$STRESS_DATA/results}"
: "${SILO_PORT:=19000}"
STRESS_UID=$(id -u)
STRESS_GID=$(id -g)
export STRESS_DATA STRESS_RESULTS SILO_PORT STRESS_UID STRESS_GID
mkdir -p "$STRESS_DATA/silo" "$STRESS_DATA/prometheus" "$STRESS_DATA/grafana" "$STRESS_RESULTS/.prometheus"

profiles=(--profile monitoring)
if [[ ${MONITORING:-1} == 0 ]]; then
    profiles=()
elif [[ $(uname -s) != Linux ]]; then
    echo "Prometheus and Grafana need Linux host networking; starting SILO only (MONITORING=0)." >&2
    profiles=()
fi
docker compose -f "$HERE/compose.yaml" ${profiles[@]+"${profiles[@]}"} up -d

for _ in $(seq 60); do
    curl -fsS "http://127.0.0.1:$SILO_PORT/minio/health/live" >/dev/null 2>&1 && break
    sleep 1
done
code=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT \
    --aws-sigv4 "aws:amz:us-east-1:s3" --user minioadmin:minioadmin \
    "http://127.0.0.1:$SILO_PORT/isledb")
case $code in
200 | 409) ;;
*) echo "creating bucket isledb: HTTP $code" >&2; exit 1 ;;
esac

cat <<MSG
SILO on 127.0.0.1:$SILO_PORT, bucket isledb. For stress runs:
  export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
  go run . run ... -out $STRESS_RESULTS
MSG
if ((${#profiles[@]})); then
    cat <<MSG
Grafana on 127.0.0.1:${GRAFANA_PORT:-3300}, Prometheus on 127.0.0.1:${PROMETHEUS_PORT:-9090}. From another machine:
  ssh -N -L ${GRAFANA_PORT:-3300}:127.0.0.1:${GRAFANA_PORT:-3300} -L ${PROMETHEUS_PORT:-9090}:127.0.0.1:${PROMETHEUS_PORT:-9090} $(hostname -f 2>/dev/null || hostname)
MSG
fi

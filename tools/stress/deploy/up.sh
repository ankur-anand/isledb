#!/usr/bin/env bash
# up.sh starts SILO and creates the isledb bucket, with a SigV4-signed PUT
# so no S3 client is needed. down.sh stops it.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
: "${STRESS_DATA:?set STRESS_DATA to a directory on the disk under test}"
: "${SILO_PORT:=19000}"
export STRESS_DATA SILO_PORT
mkdir -p "$STRESS_DATA/silo"
docker compose -f "$HERE/compose.yaml" up -d
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
MSG

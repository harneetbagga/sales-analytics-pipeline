#!/usr/bin/env bash
# Apply scripts/s3_lifecycle.json to the staging bucket.
#
#   STAGING_BUCKET=<bucket> scripts/apply_s3_lifecycle.sh [--replace]
#
# put-bucket-lifecycle-configuration REPLACES whatever configuration the bucket has, so the
# script stops if one exists unless you pass --replace. Needs s3:GetLifecycleConfiguration
# and s3:PutLifecycleConfiguration. Raw files are never expired: they are the replay source.
set -euo pipefail

BUCKET="${STAGING_BUCKET:-sales-analyical-pipeline}"   # (sic) matches the real bucket name
CONFIG="$(cd "$(dirname "$0")" && pwd)/s3_lifecycle.json"

if out=$(aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET" 2>&1); then
  has_config=1
elif [[ "$out" == *NoSuchLifecycleConfiguration* ]]; then
  has_config=0
else
  echo "Could not read the current lifecycle configuration of $BUCKET:" >&2
  echo "$out" >&2
  exit 1
fi

if [[ "$has_config" == 1 && "${1:-}" != "--replace" ]]; then
  echo "$BUCKET already has a lifecycle configuration:" >&2
  echo "$out" >&2
  echo "Review it, then re-run with --replace to overwrite it." >&2
  exit 1
fi

aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET" --lifecycle-configuration "file://$CONFIG"
echo "Applied $CONFIG to s3://$BUCKET. Current configuration:"
aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET"

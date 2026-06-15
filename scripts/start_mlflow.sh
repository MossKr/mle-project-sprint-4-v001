#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ -f "$ROOT_DIR/.env" ]; then
  export $(grep -v '^#' "$ROOT_DIR/.env" | xargs)
fi

if [ -n "$DB_DESTINATION_HOST" ] && [ -n "$DB_DESTINATION_USER" ]; then
  BACKEND="postgresql://${DB_DESTINATION_USER}:${DB_DESTINATION_PASSWORD}@${DB_DESTINATION_HOST}:${DB_DESTINATION_PORT}/${DB_DESTINATION_NAME}"
  ARTIFACTS="s3://${S3_BUCKET_NAME}/mlflow-artifacts"
  echo "backend: postgresql, artifacts: s3"
else
  BACKEND="sqlite:///${ROOT_DIR}/mlflow.db"
  ARTIFACTS="${ROOT_DIR}/mlartifacts"
  echo "backend: sqlite (local)"
fi

mlflow server \
  --backend-store-uri "$BACKEND" \
  --default-artifact-root "$ARTIFACTS" \
  --host 0.0.0.0 \
  --port 5000

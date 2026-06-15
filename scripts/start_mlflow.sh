#!/usr/bin/env bash
# Запуск MLflow tracking server.
# Backend - destination postgres, артефакты - S3 бакет студента.
set -e

export $(grep -v '^#' .env | xargs)

mlflow server \
  --backend-store-uri "postgresql://${DB_DESTINATION_USER}:${DB_DESTINATION_PASSWORD}@${DB_DESTINATION_HOST}:${DB_DESTINATION_PORT}/${DB_DESTINATION_NAME}" \
  --default-artifact-root "s3://${S3_BUCKET_NAME}/mlflow-artifacts" \
  --host 0.0.0.0 \
  --port 5000

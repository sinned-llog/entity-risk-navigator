#!/usr/bin/env bash

set -euo pipefail

DBT_DIR="/app/dbt"
PROFILES_DIR="/app/dbt"

echo "Starting required services..."
docker compose up -d postgres minio ingestion dashboard

echo "Running dbt seed..."
docker compose exec ingestion sh -c "cd ${DBT_DIR} && dbt seed --select pipeline_expected_tables --profiles-dir ${PROFILES_DIR}"

echo "Running staging models..."
docker compose exec ingestion sh -c "cd ${DBT_DIR} && dbt run --select path:models/staging --profiles-dir ${PROFILES_DIR}"

echo "Running staging_int models..."
docker compose exec ingestion sh -c "cd ${DBT_DIR} && dbt run --select path:models/staging_int --profiles-dir ${PROFILES_DIR}"

echo "Running mart models..."
docker compose exec ingestion sh -c "cd ${DBT_DIR} && dbt run --select path:models/marts --profiles-dir ${PROFILES_DIR}"

echo "Running dbt tests..."
docker compose exec ingestion sh -c "cd ${DBT_DIR} && dbt test --profiles-dir ${PROFILES_DIR}"

echo "Restarting dashboard..."
docker restart entity_risk_dashboard

echo "Pipeline completed."
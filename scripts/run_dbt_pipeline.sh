#!/usr/bin/env bash

set -euo pipefail

echo "============================================================"
echo " EntityRisk Navigator - Audited dbt Pipeline Run"
echo "============================================================"

echo ""
echo "[1/8] Starting required services..."
docker compose up -d postgres minio ingestion dashboard

echo ""
echo "[2/8] Running dbt seed: pipeline_expected_tables..."
docker compose exec ingestion sh -c "cd /app/dbt && dbt seed --select pipeline_expected_tables --profiles-dir /app/dbt"

echo ""
echo "[3/8] Running staging models through audited runner..."
docker compose exec ingestion python -m staging.run_dbt_staging

echo ""
echo "[4/8] Running staging_int models through audited runner..."
docker compose exec ingestion python -m staging_int.run_dbt_staging_int_full

echo ""
echo "[5/8] Running mart models through audited runner..."
docker compose exec ingestion python -m mart.run_dbt_mart

echo ""
echo "[6/8] Running dbt tests..."
docker compose exec ingestion sh -c "cd /app/dbt && dbt test --profiles-dir /app/dbt"

echo ""
echo "[7/8] Refreshing pipeline audit status mart..."
docker compose exec ingestion python -m mart.run_dbt_mart

echo ""
echo "[8/8] Restarting Streamlit dashboard..."
docker restart entity_risk_dashboard

echo ""
echo "============================================================"
echo " Audited dbt pipeline completed successfully."
echo "============================================================"
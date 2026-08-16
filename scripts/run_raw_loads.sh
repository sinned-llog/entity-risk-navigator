#!/usr/bin/env bash

set -euo pipefail

echo "============================================================"
echo " EntityRisk Navigator - Raw Loads"
echo "============================================================"

echo ""
echo "[1/9] Starting required services..."
docker compose up -d postgres minio ingestion

echo ""
echo "[2/9] Downloading ECB files to MinIO..."
docker compose exec ingestion python -m ingestion.download_ecb

echo ""
echo "[3/9] Loading ECB raw data into Postgres..."
docker compose exec ingestion python -m loading.load_ecb_raw_v1

echo ""
echo "[4/9] Downloading EU FSF files to MinIO..."
docker compose exec ingestion python -m ingestion.download_eu_fsf_csv

echo ""
echo "[5/9] Loading EU FSF raw data into Postgres..."
docker compose exec ingestion python -m loading.load_eu_fsf_raw_v1

echo ""
echo "[6/9] Downloading OpenSanctions files to MinIO..."
docker compose exec ingestion python -m ingestion.download_opensanctions

echo ""
echo "[7/9] Loading OpenSanctions raw data into Postgres..."
docker compose exec ingestion python -m loading.load_opensanctions_raw_v1

echo ""
echo "[8/9] Downloading GLEIF files to MinIO..."
docker compose exec ingestion python -m ingestion.download_gleif

echo ""
echo "[9/9] Loading GLEIF raw data into Postgres..."
docker compose exec ingestion python -m loading.load_gleif_raw_v1

echo ""
echo "============================================================"
echo " Raw loads completed successfully."
echo "============================================================"
#!/usr/bin/env bash

set -euo pipefail

echo "============================================================"
echo " EntityRisk Navigator - Full Pipeline"
echo "============================================================"

./scripts/run_raw_loads.sh
./scripts/run_dbt_pipeline.sh

echo ""
echo "Full pipeline completed successfully."
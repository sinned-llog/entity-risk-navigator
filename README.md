# EntityRisk Navigator

EntityRisk Navigator is an educational data engineering project for building open-data-based counterparty-risk context. It combines legal-entity, corporate-relationship, sanctions, and macroeconomic data into auditable, dashboard-ready PostgreSQL marts.

## What it does

- Ingests GLEIF LEI and Relationship Records, EU Financial Sanctions Files, OpenSanctions, and ECB Data Portal series.
- Retains source snapshots, hashes, metadata, and manifests in MinIO.
- Loads source records into PostgreSQL and transforms them with dbt through `raw`, `staging`, `staging_int`, and `marts` layers.
- Produces entity master data, parent-relationship context, sanctions screening matches, risk scores, macro context, and pipeline-status marts.
- Presents risk, entity, macro, and operational views through a Streamlit dashboard.

BaFin enrichment code is present but deferred: it is not included in the standard pipeline run.

## Architecture

Fact_KI_Anschluesse

The Docker runtime includes PostgreSQL, MinIO, an ingestion container, and the Streamlit dashboard. PostgreSQL is exposed on port `5432`, MinIO on ports `9000` and `9001`, and Streamlit on port `8501`.

## Running the pipeline

An externally managed cron job runs the full pipeline once per day:

```bash
./scripts/run_full_pipeline.sh
```

This script downloads and loads active sources, runs dbt transformations and tests, refreshes the audit-status mart, and restarts the dashboard. Individual entry points are `scripts/run_raw_loads.sh` and `scripts/run_dbt_pipeline.sh`.

## Audit and health

Every download, raw load, and dbt model run is recorded in `audit.job_runs` with status, timestamps, provenance, freshness, row counts, warnings, and errors. `marts.mart_pipeline_audit_status` exposes the latest result per pipeline target.

Check both health and freshness in the Pipeline Status dashboard page:

- `healthy`: the latest run succeeded.
- `latest_failed_previous_success_available`: the latest run failed, but a prior successful output exists.
- `not_healthy`: the latest run failed or is incomplete and no prior successful output exists.
- `fresh`, `stale`, and `unknown`: source-data freshness state, evaluated independently of run completion.

## Documentation

- [Architecture](docs/architecture.md)
- [Data sources](docs/data_sources.md)
- [Pipeline design](docs/pipeline_design.md)

## Disclaimer

This is a private, non-commercial educational project. It does not provide legal, regulatory, AML, KYC, sanctions, or financial advice. Risk indicators and sanctions matches are open-data-based screening signals and require appropriate validation before operational use.
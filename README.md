## EntityRisk Navigator

EntityRisk navigator is a private educational data engineering project.

The project builds an open-data-based counterparty risk intelligence pipeline using public financial, regulatory and sanctions-related datasets.

## Core Data Sources

- GLEIF LEI Data
- EU Financial Sanctions Files
- OpenSanctions Consolidated Sanctions
- ECB Data Portal
- BaFin Unternehmensdatenbank

## Architecture

Public data sources are ingested into a local S3-compatible raw layer using MinIO.
Structured data is loaded into PostgreSQL and transformed into staging and mart layers.
The project will provide a Streamlit dashboard and a FastAPI interface.

## Planned Components

- MinIO raw / bronze layer
- PostgreSQL data warehouse
- dbt transformations
- Streamlit dashboard
- FastAPI service
- Daily delta pipeline
- Weekly full refresh pipeline
- Optional Neo4j relationship graph

## Disclaimer

This project is for private, non-commercial educational purposes only.
It does not provide legal, regulatory, AML, KYC, sanctions or financial advice.
All risk indicators are open-data-based signals intended for learning and demonstration.
EOF
# Data Sources

## Scope and provenance

This catalogue lists the sources implemented in the repository. Downloaded files are retained in MinIO with a source URL, retrieval metadata, SHA-256 hash, quality-check results, and a per-run manifest. The raw loaders use successful manifests to locate the source snapshot they load.

URLs supplied through environment variables are intentionally documented as configuration rather than inferred values. In particular, the repository does not provide a value for `EU_FSF_CSV_FULL_URL`.

| Source | Dataset | Role | Format and retrieval | Raw target | Current status |
| --- | --- | --- | --- | --- | --- |
| GLEIF | LEI Golden Copy | Entity master data | Full ZIP/CSV snapshot; optional LastDay delta | `raw.gleif_lei_full` | Active |
| GLEIF | Relationship Records Golden Copy | Direct and ultimate parent relationships | Full ZIP/CSV snapshot | `raw.gleif_rr_full` | Active |
| European Commission | EU Financial Sanctions Files | EU sanctions subjects, aliases, regulations, and context | Full CSV, URL configured through environment | `raw.eu_fsf_full` | Active when URL is configured |
| OpenSanctions | Consolidated sanctions targets | Cross-source sanctions targets and aliases | Full `targets.simple.csv` bulk download | `raw.opensanctions_targets` | Active |
| European Central Bank | ECB Data Portal series | Macroeconomic context | Per-series CSV API responses | `raw.ecb_observations_full` | Active |
| BaFin | Unternehmensdatenbank | Candidate-specific enrichment | Candidate-driven HTML retrieval | No active raw target in standard pipeline | Deferred / inactive in MVP |

## GLEIF Golden Copy

### LEI Golden Copy

| Attribute | Detail |
| --- | --- |
| Provider | Global Legal Entity Identifier Foundation (GLEIF) |
| Configured endpoint | `GLEIF_LEI_FULL_URL`; example configuration points to `https://leidata-preview.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv` |
| Optional delta endpoint | `GLEIF_LEI_DELTA_LASTDAY_URL`; disabled by default through `GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY=false` |
| Default mode | Full snapshot, enabled through `GLEIF_DOWNLOAD_LEI_FULL=true` |
| Format | ZIP archive containing CSV |
| Raw target | `raw.gleif_lei_full` |
| Main downstream use | GLEIF-based entity candidates and the entity master mart |

The staging model retains the LEI, legal name and normalized name, entity and registration status, legal jurisdiction, legal and headquarters address countries, renewal date, update date, and source load date. It validates LEI-shaped identifiers and deduplicates records for downstream entity processing.

### Relationship Records Golden Copy

| Attribute | Detail |
| --- | --- |
| Provider | GLEIF |
| Configured endpoint | `GLEIF_RR_FULL_URL`; example configuration points to `https://leidata-preview.gleif.org/api/v2/golden-copies/publishes/rr/latest.csv` |
| Default mode | Full snapshot, enabled through `GLEIF_DOWNLOAD_RR_FULL=true` |
| Format | ZIP archive containing CSV |
| Raw target | `raw.gleif_rr_full` |
| Main downstream use | Direct and ultimate parent summaries, relationship edges, paths, and relationship context marts |

The records provide start and end node identifiers and types plus relationship type. The pipeline uses them to enrich GLEIF entity candidates with parent organization context.

For both GLEIF datasets, the downloader requires a ZIP extension, a CSV entry within the archive, and a minimum file size. It uses a configurable timeout, 1 MiB download chunks by default, and three retries with a five-second wait by default. The loader supports configured freshness handling and batch insertion.

## EU Financial Sanctions Files

| Attribute | Detail |
| --- | --- |
| Provider | European Commission / EU Financial Sanctions Files |
| Endpoint | `EU_FSF_CSV_FULL_URL` must be provided in the runtime environment |
| Mode | Full consolidated list |
| Format | CSV |
| Raw target | `raw.eu_fsf_full` |
| Main downstream use | Normalized EU sanctions subjects and names; unified sanctions screening |

The staging layer extracts source logical IDs, EU and UN references, subject type, name aliases, address and country fields, citizenship and birth-date information where present, regulation/programme context, publication date, and publication URL. It derives stable normalized records for subject- and name-level models.

The downloader checks CSV extension, a minimum size of 1,000 bytes, and a UTF-8 sample. Its default HTTP timeout is 300 seconds, with three retries and a five-second wait.

## OpenSanctions

| Attribute | Detail |
| --- | --- |
| Provider | OpenSanctions |
| Configured endpoint | `OPENSANCTIONS_SANCTIONS_TARGETS_URL`; example configuration is `https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv` |
| Mode | Full consolidated sanctions-target bulk download |
| Format | CSV |
| Raw target | `raw.opensanctions_targets` |
| Main downstream use | Sanctions target and alias normalization; unified sanctions screening |

The required input columns are `id`, `schema`, and `name`. The loader and downstream models use the target identifier and schema, names and aliases, countries, addresses, identifiers, sanctions context, and available temporal fields. The source contributes organization and legal-entity-oriented records to sanctions matching.

Quality checks require CSV, a minimum size of 1,000 bytes, UTF-8 sample readability, and the required columns. Defaults are a 900-second timeout, 1 MiB chunks, three retries, and a five-second wait.

## ECB Data Portal

| Attribute | Detail |
| --- | --- |
| Provider | European Central Bank (ECB) Data Portal |
| API base URL | `ECB_API_BASE_URL`, default `https://data-api.ecb.europa.eu/service` |
| Retrieval | One CSV request per enabled series, from `ECB_START_PERIOD` (default `2020-01-01`) |
| Raw target | `raw.ecb_observations_full` |
| Required response fields | `TIME_PERIOD`, `OBS_VALUE` |
| Main downstream use | Macro time series, macro-pressure score, and entity macro context marts |

Enabled default series are:

| Flow | Default series key | Indicator | Frequency |
| --- | --- | --- | --- |
| MIR | `M.U2.B.A2I.AM.R.A.2240.EUR.N` | Cost of borrowing for corporations | Monthly |
| EST | `B.EU000A2X2A25.WT` | Euro short-term rate | Business daily |
| YC | `B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y` | AAA yield curve, 2-year spot rate | Business daily |
| YC | `B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y` | AAA yield curve, 10-year spot rate | Business daily |

Two BSI series are optional and are enabled only when their environment variables are non-empty: loans to non-financial corporations (`ECB_BSI_LOANS_NFC`) and monetary aggregate M3 (`ECB_BSI_M3`). The downloader validates CSV format, required columns, a minimum size of 50 bytes, and a UTF-8 sample. Defaults are a 300-second timeout, 1 MiB chunks, three retries, and a five-second wait.

## BaFin Unternehmensdatenbank

The repository contains `bafin_scraper.py`, a candidate-driven downloader for BaFin institution pages. Candidates come from `BAFIN_CANDIDATES_FILE` (default `/app/config/bafin_candidates.csv`) and may supply a BaFin detail URL, institution ID, or a configurable search URL template. The module limits a run to 20 candidates by default and waits three seconds between requests.

This path is not invoked by `scripts/run_raw_loads.sh`; `staging.stg_bafin_pages_full` is inactive and not required for the current MVP. It is therefore documented as implemented-but-deferred rather than as an active production source.

## Cross-source quality and limitations

All implemented downloaders record result metadata and run quality checks before upload. Depending on source, checks include allowed extensions, minimum byte size, ZIP-to-CSV verification, UTF-8 sample decoding, and required columns. A failed quality check prevents that file from being reported as successfully downloaded.

The pipeline preserves source provenance but does not establish legal status. Exact normalized-name matches, contextual country and identifier checks, and match-quality tiers support screening prioritization only. Users must validate potential matches against authoritative source records and their applicable compliance process.

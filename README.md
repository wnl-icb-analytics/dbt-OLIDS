# dbt-olids

Foundational data layers for OLIDS (One London Integrated Data Set).

## Two Pipelines

| Tree | Feed | Coverage | Refresh | Publishes to |
|---|---|---|---|---|
| `models/olids` | `Data_Store_OLIDS_WNL` | NCL practices (production) | Nightly | `OLIDS_ENGINEERING` (`LANDING`/`CONFORMED`/`STABLE`) → `DATA_LAKE.OLIDS` |
| `models/synapse` | `Data_Store_OLIDS` (legacy Synapse) | Full NCL, frozen since 16 July 2026 | None (upstream stopped) | `OLIDS_ENGINEERING` (`SYNAPSE_BASE`/`SYNAPSE_STABLE`) → `DATA_LAKE.OLIDS_SYNAPSE` |

Run selectors:

```bash
dbt run --exclude tag:synapse  # nightly (new pipeline)
dbt run -s tag:synapse         # legacy refresh
```

## What This Project Does

Builds two data layers:

**Conformed Layer**
Filtered views of OLIDS source tables applying:
- NCL practice filtering
- Sensitive patient exclusion
- Concept mapping for clinical codes

**Stable Layer**
Tables that expose the stable analytical interface. Includes:
- Indexed patient and person ids
- WNL patient filtering
- Clustering on key columns where useful

Analytical models built on the stable layer: [dbt-ncl-analytics](https://github.com/ncl-icb-analytics/dbt-ncl-analytics)

## Quick Start

```bash
# Clone and setup
git clone https://github.com/ncl-icb-analytics/dbt-olids
cd dbt-olids
python -m venv venv
pip install -r requirements.txt

# Configure connection
cp profiles.yml.template profiles.yml
cp env.example .env
# Edit .env with your Snowflake credentials

# Activate environment (run this every session)
.\start_dbt.ps1

# Build
dbt deps
dbt run  # Builds all models
```

## Common Commands

**Always start with:** `.\start_dbt.ps1` (loads credentials from `.env`)

```bash
# Regular development runs (use XS-sized warehouse)
dbt run                   # Build all models
dbt run -s tag:conformed  # Conformed layer only

# Tests are run separately when needed
dbt test -s stable_patient
dbt test -s tag:stable

# Full refresh of stable layer (use L-sized warehouse)
dbt run --full-refresh
```

Deep QA lives in `scripts/checks`. Elementary will cover later monitoring.

**Warehouse sizing:**
- Regular runs: XS-sized warehouse in `.env`
- Full refresh: L-sized warehouse in `.env`

## Configuration

Copy templates and configure your Snowflake connection:

```bash
cp profiles.yml.template profiles.yml
cp env.example .env
# Edit .env: account, user, role, database, warehouse
dbt debug  # Test connection
```

**Prerequisites:**
- Snowflake access with the ISL-USERGROUP-SECONDEES-NCL role
- Access to DATA_LAB_OLIDS_NCL and Data_Store_OLIDS_Clinical_Validation databases

Never commit `.env` or `profiles.yml`.

## Project Structure

```
models/olids/
├── landing/        # Source cache tables
├── conformed/      # Filtered views
├── stable/         # Published tables
└── intermediate/   # Practices lookup, enriched concept map
```

## Where Objects Are Built

All models are built in the database specified by `SNOWFLAKE_TARGET_DATABASE` in your `.env` file (typically `DATA_LAB_OLIDS_NCL`):

- **Landing**: `LANDING.*` (tables)
- **Conformed layer**: `CONFORMED.*` (views)
- **Stable layer**: `STABLE.*` (tables)
- **Intermediate**: `CONFORMED.*` (tables)

The stable layer reads from `Data_Store_OLIDS_Clinical_Validation` source tables.

## Contributing

See [Contributing Guide](CONTRIBUTING.md) for workflow details.

## License

Dual licensed under Open Government v3 & MIT. All code outputs subject to Crown Copyright.

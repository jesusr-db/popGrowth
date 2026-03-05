# Store Siting App

QSR store siting app that scores US counties by leading indicators of population growth.

## Quick Start

1. Configure Databricks CLI: `databricks configure`
2. Download county GeoJSON: `python scripts/download_geojson.py`
3. Deploy: `databricks bundle deploy -t dev`
4. Run ingestion jobs: `databricks bundle run ingest_building_permits -t dev`
5. Run Silver transforms: `databricks bundle run transform_silver -t dev`
6. Run Gold scoring: `databricks bundle run compute_gold_scores -t dev`
7. Access app at the Databricks Apps URL

## Architecture

Bronze (10 open data sources) -> Silver (county-level FIPS standardized) -> Gold (composite growth score) -> React + FastAPI App (choropleth map)

## Data Sources

- Census Building Permits (monthly)
- HUD Residential Construction Permits (quarterly)
- USPS Migration via HUD (quarterly)
- USPS Vacancy via HUD (quarterly)
- BLS QCEW Employment (quarterly)
- NCES School Enrollment (annual)
- ACS 1-Year Estimates (annual)
- Census County Business Patterns (annual)
- County-Level SSP Projections (periodic)
- Census National Population Projections (periodic)

## Testing

```bash
python -m pytest tests/ -v              # Python tests
cd src/app/frontend && npx vitest run   # Frontend tests
```

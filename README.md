# Faker Data Pipeline with dbt

A data pipeline project that generates fake personal data, processes it using dbt (data build tool), and generates quality and analytics reports.

## Prerequisites

- Python 3.11+
- Poetry for dependency management
- DuckDB
- dbt-core and dbt-duckdb
- Make

## Installation

1. Clone the repository
2. Install dependencies:


```bash
make install
```

3. Configure dbt:


```bash
cp .dbt/profiles.yml.example .dbt/profiles.yml
```

## Usage

The pipeline can be run with individual commands or as a complete pipeline:

### Individual Steps

```bash
make fetch-data
make run-dbt
make reports
```

### Complete Pipeline

Run all steps in sequence:

```bash
make pipeline
```

This will:
1. Clean existing data
2. Run quality checks
3. Run tests
4. Generate and fetch data
5. Run dbt models
6. Generate reports

## Data Models

All models are materialized as views in DuckDB:

### Staging
- `stg_persons`: Initial staging of raw person data

### Marts
- `dim_age_groups`: Age demographics and Gmail usage
- `dim_location`: Country-level Gmail adoption
- `fact_email_usage`: Email provider usage by demographics

## Reports

The pipeline generates two dashboards:

### Data Quality Dashboard
- Record counts and quality scoring
- Field-level completeness, uniqueness, and validity metrics
- PII masking verification

### Analytics Dashboard
- Gmail usage statistics
- Age group analysis
- Top countries by Gmail usage

## Development

### Code Quality
```bash
make quality  # Runs black, isort, and mypy
```

### Testing
```bash
make test    # Runs pytest suite
```

## Docker Support

Build and run the pipeline in a container:
```bash
docker build -t faker-pipeline .
docker run faker-pipeline
```

## Environment Variables

- `GENDER`: Filter for data generation (default: male)
- `BATCH_SIZE`: Records per batch (default: 1000)
- `TOTAL`: Total records to generate (default: 30000)




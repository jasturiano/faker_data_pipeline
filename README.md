# Faker Data Pipeline

A data pipeline for generating, processing, and analyzing synthetic personal data using the Faker API.

## Overview

This pipeline:
1. Fetches synthetic data from the Faker API
2. Anonymizes PII
3. Stores data in DuckDB
4. Generates quality metrics and reports

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/faker_data_pipeline.git
   cd faker_data_pipeline
   ```

2. **Install dependencies:**
   ```bash
   poetry install
   ```

3. **Run tests:**
   ```bash
   make test
   ```

## Usage

### Running the Pipeline

Run the entire pipeline:
```bash
make pipeline
```

### Docker

1. **Build the Docker image:**
   ```bash
   docker build -t faker-pipeline .
   ```

2. **Run the Docker container:**
   ```bash
   docker run --rm faker-pipeline
   ```

### Customization

Adjust environment variables for Docker:
```bash
docker run --rm -e GENDER=female -e BATCH_SIZE=500 -e TOTAL=1000 faker-pipeline
```

## Development

### Code Quality

Run quality checks:
```bash
make quality
```

### Testing

Run tests:
```bash
make test
```

## CI/CD

The project uses GitHub Actions for automated testing and quality checks.






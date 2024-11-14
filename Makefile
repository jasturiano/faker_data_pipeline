.PHONY: install quality test pipeline fetch-data reports clean

GENDER ?= male
BATCH_SIZE ?= 1000
BIRTHDAY_START ?= 1950-01-01
TOTAL ?= 30000

# Basic setup
install:
	poetry install

# Code quality
quality:
	@echo "Running code quality checks..."
	poetry run black . --check
	poetry run isort . 
	poetry run mypy data_pipeline/

# Run tests
test:
	@echo "Running tests..."
	poetry run pytest

# Data pipeline steps
fetch-data:
	@echo "🔍 Fetching data..."
	poetry run python -m data_pipeline.data_ingestion \
		--gender $(GENDER) \
		--batch_size $(BATCH_SIZE) \
		--total $(TOTAL)

# Generate reports
reports:
	@echo "Generating reports..."
	poetry run python -m data_pipeline.report_generation

# Clean data
clean:
	@echo "Cleaning previous data..."
	rm -f *.duckdb

# Full pipeline
pipeline:
	./scripts/run_pipeline.sh

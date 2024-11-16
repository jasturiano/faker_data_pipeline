.PHONY: install quality test pipeline fetch-data dbt deps reports clean

GENDER ?= male
BATCH_SIZE ?= 1000
TOTAL ?= 30000


PROJECT_DIR := $(shell pwd)

install:
	poetry install

deps:
	poetry run dbt deps --profiles-dir $(PROJECT_DIR)/.dbt

quality:
	poetry run black . --check
	poetry run isort . 
	poetry run mypy data_pipeline/

test:
	poetry run pytest data_pipeline/tests/ --ignore=dbt_packages/

fetch-data:
	poetry run python -m data_pipeline.data_ingestion \
		--gender $(GENDER) \
		--batch_size $(BATCH_SIZE) \
		--total $(TOTAL) \
	

dbt:
	poetry run dbt run --profiles-dir $(PROJECT_DIR)/.dbt
	poetry run dbt test --profiles-dir $(PROJECT_DIR)/.dbt
	

reports:
	poetry run python -m data_pipeline.report_generation

clean:
	rm -f *.duckdb

pipeline: clean quality test fetch-data deps dbt reports

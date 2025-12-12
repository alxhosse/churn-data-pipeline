.PHONY: help setup install-deps setup-db start-db stop-db logs-db load-data run-analysis calculate-metrics run-metrics create-dataset run-dataset clean clean-all clean-db clean-output

# Python command - defaults to venv python if available, otherwise python3
# Note: Scripts handle their own path setup
# Ensure dependencies are synced first: uv sync --no-install-project
VENV_PYTHON := $(shell [ -d .venv ] && echo ".venv/bin/python" || echo "python3")
PYTHON ?= $(VENV_PYTHON)

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ''
	@echo 'Python: $(PYTHON)'
	@echo 'Note: Run "make install-deps" first to install dependencies'

install-deps: ## Install Python dependencies using uv
	uv sync --no-install-project
	@echo "Dependencies installed! Python: $(PYTHON)"

setup-db: ## Start PostgreSQL database using Docker
	docker-compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5
	@until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do \
		echo "Waiting for PostgreSQL..."; \
		sleep 2; \
	done
	@echo "PostgreSQL is ready!"
	@echo ""
	@echo "Database connection details:"
	@echo "  Host: localhost"
	@echo "  Port: 5432"
	@echo "  Database: churn"
	@echo "  User: postgres"
	@echo "  Password: postgres"

start-db: ## Start PostgreSQL database (alias for setup-db)
	$(MAKE) setup-db

stop-db: ## Stop PostgreSQL database
	docker-compose down

logs-db: ## View PostgreSQL logs
	docker-compose logs -f postgres

load-data: ## Load CSV data into database (requires DATA_FILE variable)
	@if [ -z "$(DATA_FILE)" ]; then \
		echo "Error: DATA_FILE variable is required"; \
		echo "Usage: make load-data DATA_FILE=data/actions2load.csv"; \
		exit 1; \
	fi
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/load_data.py $(DATA_FILE) \
		--dbname churn \
		--user postgres \
		--password postgres

run-analysis: ## Run milestone 1 analysis (requires START_DATE and END_DATE variables)
	@if [ -z "$(START_DATE)" ] || [ -z "$(END_DATE)" ]; then \
		echo "Error: START_DATE and END_DATE variables are required"; \
		echo "Usage: make run-analysis START_DATE=2020-01-01 END_DATE=2023-12-31"; \
		exit 1; \
	fi
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/run_analysis.py \
		--start-date $(START_DATE) \
		--end-date $(END_DATE) \
		--dbname churn \
		--user postgres \
		--password postgres \
		--output-dir output

calculate-metrics: ## Calculate customer metrics (requires START_DATE and END_DATE variables)
	@if [ -z "$(START_DATE)" ] || [ -z "$(END_DATE)" ]; then \
		echo "Error: START_DATE and END_DATE variables are required"; \
		echo "Usage: make calculate-metrics START_DATE=2020-01-01 END_DATE=2023-12-31"; \
		exit 1; \
	fi
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/calculate_metrics.py \
		--start-date $(START_DATE) \
		--end-date $(END_DATE) \
		--dbname churn \
		--user postgres \
		--password postgres

run-metrics: ## Run milestone 2: calculate and analyze metrics (requires START_DATE and END_DATE variables)
	@if [ -z "$(START_DATE)" ] || [ -z "$(END_DATE)" ]; then \
		echo "Error: START_DATE and END_DATE variables are required"; \
		echo "Usage: make run-metrics START_DATE=2020-01-01 END_DATE=2023-12-31"; \
		exit 1; \
	fi
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/run_metrics.py \
		--start-date $(START_DATE) \
		--end-date $(END_DATE) \
		--dbname churn \
		--user postgres \
		--password postgres \
		--output-dir output

create-dataset: ## Create current customer dataset (Milestone 3)
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/create_dataset.py \
		--output output/current_customer_dataset.csv \
		--dbname churn \
		--user postgres \
		--password postgres

run-dataset: ## Run milestone 3: create dataset and calculate statistics
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/run_dataset.py \
		--output-dir output \
		--dbname churn \
		--user postgres \
		--password postgres

clean: ## Remove output files and stop database
	docker-compose down -v
	rm -rf output/
	rm -f *.png *.csv

clean-all: ## Clean everything: database schema and all output files (requires confirmation)
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/cleanup.py \
		--dbname churn \
		--user postgres \
		--password postgres \
		--output-dir output

clean-db: ## Clean only database schema (keeps output files)
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/cleanup.py \
		--db-only \
		--dbname churn \
		--user postgres \
		--password postgres

clean-output: ## Clean only output files (keeps database)
	PYTHONPATH=src:$$PYTHONPATH $(PYTHON) scripts/cleanup.py --output-only --output-dir output



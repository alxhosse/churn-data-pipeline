# Churn Data Pipeline

A comprehensive data pipeline project for analyzing customer churn data. This project provides tools to load event data, calculate customer metrics, and create analytical datasets for churn analysis.

## Project Structure

```
churn-data-pipeline/
├── data/                    # Data files (CSV datasets)
├── sql/                     # SQL queries
│   ├── create_schema.sql   # Database schema creation
│   ├── events_per_account.sql
│   └── events_per_day.sql
├── src/                     # Python source code
│   ├── database.py         # Database connection utilities
│   ├── load_data.py        # CSV data loading script
│   ├── visualize_events.py # Event visualization
│   ├── run_analysis.py     # Milestone 1 analysis script
│   ├── calculate_metrics.py # Metric calculation (Milestone 2)
│   ├── metric_analysis.py  # Metric analysis and visualization
│   ├── run_metrics.py       # Milestone 2 main script
│   ├── create_dataset.py   # Dataset creation (Milestone 3)
│   ├── dataset_stats.py    # Dataset statistics
│   ├── run_dataset.py      # Milestone 3 main script
│   └── cleanup.py          # Cleanup utilities
├── scripts/                 # Standalone executable scripts
│   ├── load_data.py        # Data loading script
│   ├── run_analysis.py      # Analysis script
│   ├── calculate_metrics.py # Metrics script
│   ├── run_metrics.py       # Metrics analysis script
│   ├── create_dataset.py   # Dataset creation script
│   ├── run_dataset.py      # Dataset script
│   └── cleanup.py          # Cleanup script
├── sql/                     # SQL queries
│   ├── create_schema.sql   # Database schema creation
│   ├── create_metric_tables.sql # Metric tables creation
│   ├── events_per_account.sql
│   ├── events_per_day.sql
│   ├── insert_count_metric.sql
│   ├── metric_coverage.sql
│   ├── metric_stats_over_time.sql
│   └── current_customer_dataset.sql
├── output/                  # Analysis outputs (created automatically)
├── data/                    # Data files (CSV datasets)
├── docker-compose.yml       # Docker Compose configuration
├── Makefile                 # Convenience commands
└── pyproject.toml          # Project dependencies
```

## Setup

### Prerequisites

- Python 3.11+ (required for numpy 2.3.5+)
- PostgreSQL database (or Docker for automated setup)
- uv package manager (recommended) or pip
- Docker and Docker Compose (optional, for automated database setup)

### Installation

1. Install dependencies using uv:
```bash
uv sync --no-install-project
```

This installs dependencies without trying to install the project package (scripts handle imports directly).

Or using pip to install dependencies only:
```bash
pip install numpy pandas psycopg2-binary scikit-learn matplotlib
```

2. Set up PostgreSQL database:

**Option A: Using Docker (Recommended - Automated Setup)**

Start PostgreSQL using Docker Compose:

Using Make (Linux/Mac):
```bash
make setup-db
```

Using scripts:
```bash
# Linux/Mac
./scripts/setup_db.sh

# Windows PowerShell
.\scripts\setup_db.ps1
```

Or manually:
```bash
docker-compose up -d postgres
```

The database will be available at:
- Host: `localhost`
- Port: `5432`
- Database: `churn`
- User: `postgres`
- Password: `postgres`

Useful Docker commands:
```bash
# Stop the database
make stop-db
# or
docker-compose down

# View logs
make logs-db
# or
docker-compose logs -f postgres

# Remove database and volumes (clean slate)
docker-compose down -v
```

**Option B: Manual PostgreSQL Setup**

If you have PostgreSQL installed locally:
```bash
createdb churn
```

3. Set environment variables (optional, or pass as arguments):

For Docker setup (default credentials):
```bash
export CHURN_DB=churn
export CHURN_DB_USER=postgres
export CHURN_DB_PASS=postgres
export CHURN_DB_HOST=localhost
```

For custom PostgreSQL setup:
```bash
export CHURN_DB=churn
export CHURN_DB_USER=postgres
export CHURN_DB_PASS=your_password
export CHURN_DB_HOST=localhost
```

## Quick Start

```bash
# 1. Install dependencies
make install-deps

# 2. Start database (Docker)
make setup-db

# 3. Download dataset from Google Drive (link in README) and place in data/
# 4. Load your data
make load-data DATA_FILE=data/actions2load.csv

# 5. Run all milestones
make run-analysis START_DATE=2020-01-01 END_DATE=2023-12-31
make run-metrics START_DATE=2020-01-01 END_DATE=2023-12-31
make run-dataset

# 6. Clean up when done
make clean-all
```

## Usage

### Milestone 1: Load Data and Perform QA Analysis

#### Step 1: Configure Database Connection

Set up your database connection using environment variables or command-line arguments.

Option 1: Environment variables (recommended)
```bash
export CHURN_DB=churn
export CHURN_DB_USER=postgres
export CHURN_DB_PASS=your_password
export CHURN_DB_HOST=localhost
```

Option 2: Use .env file (copy `.env.example` to `.env` and fill in values)

#### Step 2: Download Dataset

**Download the dataset from Google Drive:**

📥 [Download actions2load.csv from Google Drive](https://drive.google.com/file/d/13zq63adaltVBBTxMPwmBq8oGpZEMCjxq/view?usp=sharing)

Place the downloaded `actions2load.csv` file in the `data/` directory.

**Note:** The dataset is too large for GitHub (353 MB), so it's hosted on Google Drive. Make sure to download it before proceeding.

#### Step 3: Load CSV Data into Database

Load your CSV file into PostgreSQL:

Using Make (recommended):
```bash
make load-data DATA_FILE=data/actions2load.csv
```

Or using scripts directly:
```bash
python scripts/load_data.py data/actions2load.csv \
    --dbname churn \
    --user postgres \
    --password your_password
```

**Important:** 
- The CSV should have columns: `account_id`, `event_time`, `event_type`, and optionally `product_id` and `additional_data`.

**Quick start with Make (if using Docker):**
```bash
# Step 1: Install dependencies
make install-deps

# Step 2: Setup database
make setup-db

# Step 3: Load your data
make load-data DATA_FILE=data/actions2load.csv

# Step 4: Run milestone 1 analysis
make run-analysis START_DATE=2020-01-01 END_DATE=2023-12-31

# Step 5: Calculate and analyze metrics (Milestone 2)
make run-metrics START_DATE=2020-01-01 END_DATE=2023-12-31

# Step 6: Create current customer dataset (Milestone 3)
make run-dataset
```

**Note:** The Makefile automatically uses `.venv/bin/python` if available (created by `uv sync`), otherwise falls back to `python3`. Run `make install-deps` first to ensure dependencies are installed.

### Cleanup and Reset

To start fresh or clean up after testing:

```bash
# Clean everything (database + output files) - requires confirmation
make clean-all

# Clean only database (keeps output files)
make clean-db

# Clean only output files (keeps database)
make clean-output

# Basic clean (stops Docker and removes output)
make clean
```

Or use the cleanup script directly:
```bash
python scripts/cleanup.py \
    --dbname churn \
    --user postgres \
    --password postgres \
    --output-dir output
```

#### Step 3: Run Analysis

Run the complete analysis suite:

```bash
python scripts/run_analysis.py \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    --dbname churn \
    --user postgres \
    --password your_password \
    --output-dir output
```

Or using module:
```bash
uv run python -m src.run_analysis \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    --dbname churn \
    --user postgres \
    --password your_password \
    --output-dir output
```

This will:
1. Generate events per account per month analysis
2. Create visualizations for the most common event type
3. Save all results to the `output/` directory

#### Step 4: Visualize Specific Event Types

To visualize a specific event type:

```bash
uv run python -m src.visualize_events "event_type_name" \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    --dbname churn \
    --user postgres \
    --password your_password \
    --output-dir output
```

## Analysis Outputs

### Milestone 1 Outputs:
- `output/events_per_account_per_month.csv` - Events per account analysis
- `output/{event_type}_events_per_day.csv` - Daily event counts
- `output/{event_type}_event_qa.png` - Visualization of events over time

### Milestone 2 Outputs:
- `output/metric_coverage.csv` - Metric coverage statistics
- `output/{metric_name}_stats_over_time.csv` - Metric statistics over time
- `output/{metric_name}_metric_qa.png` - Metric QA visualization

### Milestone 3 Outputs:
- `output/current_customer_dataset.csv` - Current customer dataset (one row per customer, one column per metric)
- `output/current_customer_dataset_summarystats.csv` - Summary statistics for the dataset

## Database Schema

The project uses a PostgreSQL schema called `churn_analytics` with the following tables:

**Event Tables:**
- `event_type` - Unique event types
- `event` - Individual events with account_id, event_time, event_type_id, product_id, additional_data

**Metric Tables:**
- `metric_name` - Unique metric names
- `metric` - Customer metrics with account_id, metric_time, metric_name_id, metric_value

## Architecture

The project uses a modular architecture with SQL queries and Python scripts:

- `sql/events_per_account.sql` - Events per account analysis query
- `sql/events_per_day.sql` - Daily events analysis query
- `src/visualize_events.py` - Event visualization utilities

The database connection utilities (`src/database.py`) provide a clean interface for PostgreSQL operations.

## Milestones

### Milestone 1: Load Data and Perform QA Analysis

The analysis answers the following questions:

**Events Per Account Analysis:**
- What events are most common?
- What events are least common?
- How many events average more than 0.05 events per customer per month?

**Events Per Day Analysis:**
- Do events happen equally every day, or are there patterns?
- Are there any gaps in the record of any events?
- Are there any events that only occur in part of the history?
- Are there any extreme outliers or anomalies in the number of events?

### Milestone 2: Calculate Basic Customer Metrics

**Metric Coverage:**
- What percentage of accounts have each metric?
- What are the average, min, and max values for each metric?
- When do metrics start and end?

**Metric Statistics Over Time:**
- How do metric averages change over time?
- Are there any anomalies or trends in the metrics?
- How many accounts have metrics at each time point?

### Milestone 3: Current Customer Dataset

**Dataset Creation:**
- Create a snapshot of all current customers with their metrics
- One row per customer, one column per metric
- Uses the latest metric_time for all customers

**Dataset Statistics:**
- What percent of customers have engaged in each behavior?
- What are the typical and maximum values for each metric?
- Summary statistics including mean, median, percentiles, skewness

## Cleanup and Maintenance

### Starting Fresh

If you need to start completely fresh (useful for testing or when data changes):

```bash
# Full cleanup - removes all database tables and output files
make clean-all

# Then start over:
make setup-db
make load-data DATA_FILE=data/your_file.csv
make run-analysis START_DATE=2020-01-01 END_DATE=2023-12-31
```

### Partial Cleanup

```bash
# Keep database, remove only output files
make clean-output

# Keep output files, remove only database
make clean-db
```

### Cleanup Options

The cleanup script supports several options:

```bash
# Clean everything with confirmation
python scripts/cleanup.py \
    --dbname churn \
    --user postgres \
    --password postgres

# Clean everything without confirmation (for scripts)
python scripts/cleanup.py \
    --dbname churn \
    --user postgres \
    --password postgres \
    --yes

# Clean only database
python scripts/cleanup.py \
    --db-only \
    --dbname churn \
    --user postgres \
    --password postgres

# Clean only output files
python scripts/cleanup.py --output-only
```

## Troubleshooting

### Common Issues

**Issue: "No metrics found"**
- Solution: Run Milestone 2 first (`make run-metrics`)

**Issue: "Schema does not exist"**
- Solution: Load data first (`make load-data DATA_FILE=...`)

**Issue: "Module not found"**
- Solution: Run `make install-deps` to install dependencies

**Issue: "Database connection failed"**
- Solution: Ensure PostgreSQL is running (`make setup-db` or start Docker)

**Issue: "Permission denied"**
- Solution: Check database credentials and permissions

### Getting Help

1. Check that all prerequisites are installed
2. Verify database is running and accessible
3. Ensure data file has correct format (account_id, event_time, event_type columns)
4. Review error messages for specific issues
5. Use `make clean-all` to start fresh if needed

## Project Workflow

The typical workflow follows three milestones:

1. **Milestone 1**: Load event data and perform quality assurance
   - Load CSV data into PostgreSQL
   - Analyze event patterns and frequencies
   - Visualize events over time

2. **Milestone 2**: Calculate customer metrics
   - Create metric tables
   - Calculate count metrics for common events
   - Analyze metric coverage and trends

3. **Milestone 3**: Create current customer dataset
   - Generate snapshot of all customers with metrics
   - Calculate summary statistics
   - Prepare data for further analysis

## Data Preparation

### Download the Dataset

The dataset (`actions2load.csv`) is too large for GitHub (353 MB), so it's hosted on Google Drive:

📥 **[Download actions2load.csv from Google Drive](https://drive.google.com/file/d/13zq63adaltVBBTxMPwmBq8oGpZEMCjxq/view?usp=sharing)**

After downloading, place the file in the `data/` directory.

### Dataset Format

Your CSV file should have the following columns:
- `account_id`: Customer account identifier
- `event_time`: Timestamp of the event
- `event_type`: Type of event
- `product_id`: (Optional) Product identifier
- `additional_data`: (Optional) Additional event data

### Loading the Data

Load your CSV file using:
```bash
make load-data DATA_FILE=data/actions2load.csv
```

## Contributing

This project follows standard Python project structure. When contributing:

1. Follow the existing code style
2. Add tests for new functionality
3. Update documentation (README.md) for new features
4. Ensure SQL queries are compatible with PostgreSQL 12+
5. Maintain data sanitization - no brand/company references in code or data

## License

MIT License


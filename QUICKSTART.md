# Quick Start Guide

## Milestone 1: Load Data and Perform QA Analysis

This guide walks you through completing Milestone 1 of the churn data pipeline project.

### Prerequisites Checklist

- [ ] Python 3.11+ installed (required for numpy 2.3.5+)
- [ ] uv installed (or use pip)
- [ ] Docker and Docker Compose installed (recommended) OR PostgreSQL installed locally
- [ ] CSV data file ready (`actions2load.csv` or similar)

### Step-by-Step Instructions

#### 1. Install Dependencies

```bash
uv sync
```

Or with pip:
```bash
pip install -e .
```

#### 2. Set Up PostgreSQL Database

**Option A: Using Docker (Recommended)**

Start PostgreSQL with Docker Compose:
```bash
# Linux/Mac
./scripts/setup_db.sh

# Windows PowerShell
.\scripts\setup_db.ps1

# Or manually
docker-compose up -d postgres
```

Wait for the database to be ready (the script will wait automatically). The database will use these default credentials:
- Database: `churn`
- User: `postgres`
- Password: `postgres`
- Host: `localhost`
- Port: `5432`

**Option B: Manual PostgreSQL Setup**

If you have PostgreSQL installed locally:
```bash
createdb churn
```

Or using psql:
```sql
CREATE DATABASE churn;
```

#### 3. Set Environment Variables (Optional)

If using Docker (default credentials):
```bash
export CHURN_DB=churn
export CHURN_DB_USER=postgres
export CHURN_DB_PASS=postgres
export CHURN_DB_HOST=localhost
```

If using custom PostgreSQL:
```bash
export CHURN_DB=churn
export CHURN_DB_USER=postgres
export CHURN_DB_PASS=your_password
export CHURN_DB_HOST=localhost
```

#### 4. Load CSV Data

Make sure your CSV file has these columns:
- `account_id` (required)
- `event_time` (required, timestamp format)
- `event_type` (required, text)
- `product_id` (optional)
- `additional_data` (optional)

If using Docker (default password is `postgres`):
```bash
make load-data DATA_FILE=data/actions2load.csv
```

Or manually:
```bash
python scripts/load_data.py data/actions2load.csv \
    --dbname churn \
    --user postgres \
    --password postgres
```

**Expected output:**
- Schema `churn_analytics` created
- Event types extracted and inserted
- Events loaded in batches
- Progress updates every 100,000 rows

#### 5. Determine Date Range

Check the date range of your data:
```sql
psql churn -c "SELECT MIN(event_time), MAX(event_time) FROM churn_analytics.event;"
```

#### 6. Run Analysis

Run the complete analysis (replace dates with your actual date range):

If using Docker:
```bash
python scripts/run_analysis.py \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    --dbname churn \
    --user postgres \
    --password postgres \
    --output-dir output
```

If using custom PostgreSQL:
```bash
python scripts/run_analysis.py \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    --dbname churn \
    --user postgres \
    --password your_password \
    --output-dir output
```

**This generates:**
1. `output/events_per_account_per_month.csv` - Analysis of events per account
2. `output/{most_common_event}_events_per_day.csv` - Daily event counts
3. `output/{most_common_event}_event_qa.png` - Visualization

#### 7. Review Results

**Events Per Account Analysis:**
- Open `output/events_per_account_per_month.csv`
- Answer these questions:
  - What events are most common? (top rows)
  - What events are least common? (bottom rows)
  - How many events average > 0.05 per customer per month? (check the count)

**Events Per Day Analysis:**
- Review the console output for:
  - Date range coverage
  - Gaps in data
  - Outliers detected
- View the PNG visualization to see:
  - Daily patterns
  - Trends over time
  - Anomalies

#### 8. Visualize Additional Event Types

To analyze a specific event type:
```bash
uv run python -m src.visualize_events "your_event_type_name" \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    --dbname churn \
    --user postgres \
    --password your_password \
    --output-dir output
```

### Troubleshooting

**Issue: CSV loading fails**
- Check CSV column names match expected format
- Verify CSV encoding (should be UTF-8)
- Check database connection credentials

**Issue: Date range errors**
- Verify your date range covers all data
- Check date format: YYYY-MM-DD
- Ensure dates are within the data range

**Issue: No events found**
- Verify schema name matches (`churn_analytics` by default)
- Check that data loaded successfully
- Verify event_type names match exactly (case-sensitive)

### Deliverables Checklist

For Milestone 1 submission, ensure you have:

- [ ] Results from `events_per_account_per_month.csv`
- [ ] At least one `events_per_day.csv` file
- [ ] At least one visualization PNG file
- [ ] Answers to all milestone questions documented

### Next Steps

After completing Milestone 1:
- Review the analysis results
- Identify data quality issues
- Prepare for metric creation (Milestone 2)


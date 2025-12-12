"""
Create current customer dataset from metrics
One row per customer, one column per metric
"""
import sys
import pandas as pd
from pathlib import Path
try:
    from .database import Database
except ImportError:
    from database import Database
from psycopg2 import sql


def get_latest_metric_time(db: Database):
    """Get the latest metric_time from the database"""
    db.set_search_path()
    db.execute("SELECT MAX(metric_time) FROM churn_analytics.metric")
    result = db.cursor.fetchone()
    if not result or not result[0]:
        raise ValueError("No metrics found in database. Run milestone 2 first.")
    return result[0]


def get_metric_names(db: Database):
    """Get all metric names from the database"""
    db.set_search_path()
    db.execute("SELECT metric_name FROM churn_analytics.metric_name ORDER BY metric_name_id")
    results = db.cursor.fetchall()
    return [row[0] for row in results]


def create_current_dataset(db: Database, output_path: str = None):
    """
    Create current customer dataset
    
    Args:
        db: Database instance
        output_path: Path to save CSV file
    
    Returns:
        DataFrame with current customer dataset
    """
    print(f"\n{'='*60}")
    print("Creating Current Customer Dataset")
    print(f"{'='*60}")
    
    # Get latest metric time
    latest_time = get_latest_metric_time(db)
    print(f"Latest metric time: {latest_time}")
    
    # Get all metric names
    metric_names = get_metric_names(db)
    if not metric_names:
        raise ValueError("No metrics found. Run milestone 2 to calculate metrics first.")
    
    print(f"Found {len(metric_names)} metrics: {', '.join(metric_names[:5])}{'...' if len(metric_names) > 5 else ''}")
    
    # Get accounts with recent events (active customers)
    db.set_search_path()
    db.execute("""
        SELECT DISTINCT account_id
        FROM churn_analytics.event e
        WHERE e.event_time >= %s - INTERVAL '90 days'
    """, (latest_time,))
    recent_accounts = [row[0] for row in db.cursor.fetchall()]
    print(f"Found {len(recent_accounts):,} active accounts (events in last 90 days)")
    
    # Get metrics for latest time
    db.execute("""
        SELECT 
            m.account_id,
            m.metric_time,
            n.metric_name,
            m.metric_value
        FROM churn_analytics.metric m
        INNER JOIN churn_analytics.metric_name n 
            ON m.metric_name_id = n.metric_name_id
        WHERE m.metric_time = %s
          AND m.account_id = ANY(%s)
        ORDER BY m.account_id, n.metric_name
    """, (latest_time, recent_accounts))
    
    results = db.cursor.fetchall()
    
    if not results:
        raise ValueError("No metrics found for latest time. Ensure metrics are calculated.")
    
    # Convert to DataFrame
    df = pd.DataFrame(results, columns=['account_id', 'metric_time', 'metric_name', 'metric_value'])
    
    # Pivot: one row per customer, one column per metric
    dataset = df.pivot_table(
        index=['account_id', 'metric_time'],
        columns='metric_name',
        values='metric_value',
        fill_value=0.0
    ).reset_index()
    
    # Rename metric_time to last_metric_time for clarity
    dataset = dataset.rename(columns={'metric_time': 'last_metric_time'})
    
    # Set index to account_id and last_metric_time
    dataset = dataset.set_index(['account_id', 'last_metric_time'])
    
    print(f"\nDataset created:")
    print(f"  Rows (customers): {len(dataset):,}")
    print(f"  Columns (metrics): {len(dataset.columns)}")
    print(f"  Columns: {', '.join(dataset.columns.tolist()[:10])}{'...' if len(dataset.columns) > 10 else ''}")
    
    if output_path:
        dataset.to_csv(output_path)
        print(f"\nSaved dataset to: {output_path}")
    
    return dataset


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Create current customer dataset')
    parser.add_argument('--output', type=str, default='output/current_customer_dataset.csv',
                       help='Output CSV file path')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    
    args = parser.parse_args()
    
    # Create output directory if needed
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    db = Database(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        schema=args.schema
    )
    
    try:
        dataset = create_current_dataset(db, str(output_path))
        print(f"\n{'='*60}")
        print("Dataset creation complete!")
        print(f"{'='*60}")
    except Exception as e:
        print(f"Error creating dataset: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


"""
Calculate and insert basic customer metrics
"""
import sys
from pathlib import Path
try:
    from .database import Database
except ImportError:
    from database import Database
from psycopg2 import sql


def create_metric_tables(db: Database):
    """Create metric and metric_name tables"""
    print("Creating metric tables...")
    db.create_schema()
    db.set_search_path()
    
    schema_sql_path = Path(__file__).parent.parent / 'sql' / 'create_metric_tables.sql'
    with open(schema_sql_path, 'r') as f:
        schema_sql = f.read()
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement:
                db.execute(statement)
    db.commit()
    print("Metric tables created")


def insert_metric_name(db: Database, metric_name: str):
    """
    Insert a metric name and return its ID
    
    Args:
        db: Database instance
        metric_name: Name of the metric
    
    Returns:
        metric_name_id
    """
    query = sql.SQL("""
        INSERT INTO churn_analytics.metric_name (metric_name)
        VALUES (%s)
        ON CONFLICT (metric_name) DO NOTHING
        RETURNING metric_name_id
    """)
    db.execute(query, (metric_name,))
    result = db.cursor.fetchone()
    
    if result:
        metric_id = result[0]
    else:
        # Metric already exists, get its ID
        query = sql.SQL("SELECT metric_name_id FROM churn_analytics.metric_name WHERE metric_name = %s")
        db.execute(query, (metric_name,))
        metric_id = db.cursor.fetchone()[0]
    
    db.commit()
    return metric_id


def insert_count_metric(db: Database, 
                       event_type_name: str,
                       metric_name: str,
                       start_date: str,
                       end_date: str):
    """
    Insert count metric for a specific event type
    
    Args:
        db: Database instance
        event_type_name: Name of event type to count
        metric_name: Name for the metric
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    """
    print(f"Calculating metric '{metric_name}' for event '{event_type_name}'...")
    
    # Get or create metric name
    metric_id = insert_metric_name(db, metric_name)
    
    # Read and execute insert SQL
    sql_path = Path(__file__).parent.parent / 'sql' / 'insert_count_metric.sql'
    with open(sql_path, 'r') as f:
        query = f.read()
    
    # Replace placeholders
    query = query.replace('%from_yyyy-mm-dd', start_date)
    query = query.replace('%to_yyyy-mm-dd', end_date)
    query = query.replace('%new_metric_id', str(metric_id))
    query = query.replace('%event2measure', event_type_name)
    
    db.set_search_path()
    db.execute(query)
    db.commit()
    
    # Get count of inserted rows
    db.execute("""
        SELECT COUNT(*) 
        FROM churn_analytics.metric 
        WHERE metric_name_id = %s
    """, (metric_id,))
    count = db.cursor.fetchone()[0]
    print(f"  Inserted {count:,} metric values for '{metric_name}'")


def calculate_metrics_for_common_events(db: Database,
                                       start_date: str,
                                       end_date: str,
                                       min_events_per_month: float = 0.05):
    """
    Calculate count metrics for common events (those with > threshold events per month)
    
    Args:
        db: Database instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_events_per_month: Minimum events per month threshold
    """
    print(f"\n{'='*60}")
    print("Calculating Metrics for Common Events")
    print(f"{'='*60}")
    
    # Get common events from events_per_account analysis
    events_sql_path = Path(__file__).parent.parent / 'sql' / 'events_per_account.sql'
    with open(events_sql_path, 'r') as f:
        query = f.read()
    
    query = query.replace('%from_yyyy-mm-dd', start_date)
    query = query.replace('%to_yyyy-mm-dd', end_date)
    
    db.set_search_path()
    db.execute(query)
    results = db.cursor.fetchall()
    
    # Filter events above threshold
    common_events = []
    for row in results:
        event_name = row[0]
        events_per_month = row[5]  # events_per_account_per_month
        if events_per_month > min_events_per_month:
            common_events.append(event_name)
    
    print(f"\nFound {len(common_events)} events above {min_events_per_month} events/month threshold")
    print(f"Events: {', '.join(common_events[:10])}{'...' if len(common_events) > 10 else ''}\n")
    
    # Calculate metrics for each common event
    for event_name in common_events:
        # Create metric name: "count_{event_name}"
        metric_name = f"count_{event_name}"
        try:
            insert_count_metric(db, event_name, metric_name, start_date, end_date)
        except Exception as e:
            print(f"  Error calculating metric for {event_name}: {e}")
            continue
    
    print(f"\nCompleted calculating metrics for {len(common_events)} event types")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate customer metrics')
    parser.add_argument('--start-date', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--event-type', type=str,
                       help='Specific event type to calculate metric for (optional)')
    parser.add_argument('--metric-name', type=str,
                       help='Metric name (required if --event-type specified)')
    parser.add_argument('--min-events-per-month', type=float, default=0.05,
                       help='Minimum events per month threshold for common events')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    
    args = parser.parse_args()
    
    db = Database(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        schema=args.schema
    )
    
    try:
        create_metric_tables(db)
        
        if args.event_type and args.metric_name:
            # Calculate metric for specific event type
            insert_count_metric(db, args.event_type, args.metric_name,
                              args.start_date, args.end_date)
        else:
            # Calculate metrics for all common events
            calculate_metrics_for_common_events(db, args.start_date, args.end_date,
                                              args.min_events_per_month)
    except Exception as e:
        print(f"Error calculating metrics: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


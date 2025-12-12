#!/usr/bin/env python
"""
Standalone script to calculate metrics
"""
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from calculate_metrics import main

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
    
    # Import here to avoid circular imports
    from calculate_metrics import (
        create_metric_tables, insert_count_metric, calculate_metrics_for_common_events
    )
    from database import Database
    
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
            insert_count_metric(db, args.event_type, args.metric_name,
                              args.start_date, args.end_date)
        else:
            calculate_metrics_for_common_events(db, args.start_date, args.end_date,
                                              args.min_events_per_month)
    except Exception as e:
        print(f"Error calculating metrics: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


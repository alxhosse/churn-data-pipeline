#!/usr/bin/env python
"""
Standalone script to load CSV data into PostgreSQL
Can be run directly without package installation
"""
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from load_data import load_csv_to_database
from database import Database

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Load CSV data into PostgreSQL')
    parser.add_argument('csv_path', type=str, help='Path to CSV file')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for inserts')
    
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
        load_csv_to_database(args.csv_path, db, batch_size=args.batch_size)
    except Exception as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


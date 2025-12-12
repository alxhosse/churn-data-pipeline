#!/usr/bin/env python
"""
Standalone script to create current customer dataset
"""
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from create_dataset import create_current_dataset
from database import Database

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
    except Exception as e:
        print(f"Error creating dataset: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


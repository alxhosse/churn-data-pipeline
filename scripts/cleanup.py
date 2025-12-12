#!/usr/bin/env python
"""
Standalone script to clean database and output files
"""
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from cleanup import cleanup_all, cleanup_database, cleanup_output_files
from database import Database

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean database and output files')
    parser.add_argument('--db-only', action='store_true',
                       help='Only clean database, keep output files')
    parser.add_argument('--output-only', action='store_true',
                       help='Only clean output files, keep database')
    parser.add_argument('--schema', type=str, default='churn_analytics',
                       help='Schema name to clean')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory to clean')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--yes', action='store_true',
                       help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    if args.output_only:
        cleanup_output_files(args.output_dir)
    elif args.db_only:
        db = Database(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
            schema=args.schema
        )
        try:
            cleanup_database(db, args.schema)
        finally:
            db.close()
    else:
        db = Database(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
            schema=args.schema
        )
        try:
            if args.yes:
                cleanup_database(db, args.schema)
                cleanup_output_files(args.output_dir)
            else:
                cleanup_all(db, args.schema, args.output_dir)
        finally:
            db.close()


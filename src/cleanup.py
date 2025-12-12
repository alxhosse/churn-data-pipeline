"""
Cleanup utility to reset the database and remove all generated files
Useful for starting fresh or testing
"""
import sys
from pathlib import Path
try:
    from .database import Database
except ImportError:
    from database import Database
from psycopg2 import sql


def cleanup_database(db: Database, schema: str = 'churn_analytics'):
    """
    Drop all tables and schema from the database
    
    Args:
        db: Database instance
        schema: Schema name to clean
    """
    print(f"\n{'='*60}")
    print("Cleaning Database")
    print(f"{'='*60}")
    
    db.connect()
    db.set_search_path()
    
    # Drop all tables in the schema
    tables = [
        'metric',
        'metric_name',
        'event',
        'event_type'
    ]
    
    for table in tables:
        try:
            query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            db.execute(query)
            print(f"  Dropped table: {schema}.{table}")
        except Exception as e:
            print(f"  Warning: Could not drop {schema}.{table}: {e}")
    
    # Drop the schema itself
    try:
        query = sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
            sql.Identifier(schema)
        )
        db.execute(query)
        db.commit()
        print(f"  Dropped schema: {schema}")
    except Exception as e:
        print(f"  Warning: Could not drop schema {schema}: {e}")
    
    print("Database cleanup complete!")


def cleanup_output_files(output_dir: str = 'output'):
    """
    Remove all output files
    
    Args:
        output_dir: Output directory to clean
    """
    print(f"\n{'='*60}")
    print("Cleaning Output Files")
    print(f"{'='*60}")
    
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"  Output directory does not exist: {output_dir}")
        return
    
    # Remove all CSV files
    csv_files = list(output_path.glob('*.csv'))
    for csv_file in csv_files:
        csv_file.unlink()
        print(f"  Removed: {csv_file.name}")
    
    # Remove all PNG files
    png_files = list(output_path.glob('*.png'))
    for png_file in png_files:
        png_file.unlink()
        print(f"  Removed: {png_file.name}")
    
    # Remove all PDF files
    pdf_files = list(output_path.glob('*.pdf'))
    for pdf_file in pdf_files:
        pdf_file.unlink()
        print(f"  Removed: {pdf_file.name}")
    
    # Try to remove the directory if empty
    try:
        if output_path.exists() and not any(output_path.iterdir()):
            output_path.rmdir()
            print(f"  Removed empty directory: {output_dir}")
    except Exception:
        pass
    
    print(f"Output cleanup complete! ({len(csv_files) + len(png_files) + len(pdf_files)} files removed)")


def cleanup_all(db: Database, 
                schema: str = 'churn_analytics',
                output_dir: str = 'output',
                keep_output_dir: bool = False):
    """
    Clean everything: database and output files
    
    Args:
        db: Database instance
        schema: Schema name to clean
        output_dir: Output directory to clean
        keep_output_dir: If True, keep the output directory structure
    """
    print(f"\n{'='*60}")
    print("FULL CLEANUP - Removing All Data and Outputs")
    print(f"{'='*60}")
    print("\n⚠️  WARNING: This will delete:")
    print(f"  - All tables in schema '{schema}'")
    print(f"  - All files in '{output_dir}'")
    print("\nThis action cannot be undone!")
    
    try:
        response = input("\nAre you sure you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cleanup cancelled.")
            return
    except KeyboardInterrupt:
        print("\nCleanup cancelled.")
        return
    
    # Clean database
    cleanup_database(db, schema)
    
    # Clean output files
    cleanup_output_files(output_dir)
    
    print(f"\n{'='*60}")
    print("Cleanup Complete!")
    print(f"{'='*60}")
    print("\nYou can now start fresh:")
    print("  1. Load your data: make load-data DATA_FILE=data/your_file.csv")
    print("  2. Run analyses: make run-analysis START_DATE=... END_DATE=...")


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
                # Skip confirmation for automated scripts
                cleanup_database(db, args.schema)
                cleanup_output_files(args.output_dir)
            else:
                cleanup_all(db, args.schema, args.output_dir)
        finally:
            db.close()


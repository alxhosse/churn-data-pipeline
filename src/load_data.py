"""
Load CSV data into PostgreSQL database
"""
import pandas as pd
import sys
from pathlib import Path
try:
    from .database import Database
except ImportError:
    from database import Database
from psycopg2.extras import execute_values
from psycopg2 import sql


def load_csv_to_database(csv_path: str, 
                        db: Database,
                        batch_size: int = 10000,
                        chunk_size: int = 100000):
    """
    Load CSV file into PostgreSQL database.
    """
    """
    Load CSV file into PostgreSQL database
    
    Args:
        csv_path: Path to CSV file
        db: Database instance
        batch_size: Number of rows to insert per batch
        chunk_size: Number of rows to read from CSV at a time
    """
    print(f"Loading data from {csv_path}...")
    
    # Create schema and tables
    db.create_schema()
    db.set_search_path()
    
    # Check if event table exists and has wrong column types
    # If account_id is INTEGER but should be TEXT, drop and recreate
    db.execute("""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'churn_analytics' 
        AND table_name = 'event' 
        AND column_name = 'account_id'
    """)
    result = db.cursor.fetchone()
    
    if result and result[0] == 'integer':
        print("Detected existing event table with INTEGER account_id. Dropping to recreate with TEXT...")
        db.execute("DROP TABLE IF EXISTS churn_analytics.event CASCADE")
        db.commit()
    
    # Read and execute schema creation SQL
    schema_sql_path = Path(__file__).parent.parent / 'sql' / 'create_schema.sql'
    with open(schema_sql_path, 'r') as f:
        schema_sql = f.read()
        # Split by semicolons and execute each statement
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement:
                db.execute(statement)
        db.commit()
    
    # First, get unique event types and insert them
    print("Extracting unique event types...")
    event_types = set()
    
    # Read CSV in chunks to get unique event types
    # Try to infer column names from first chunk
    chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)
    first_chunk = next(chunk_iter)
    
    # Check required columns
    required_cols = ['account_id', 'event_time', 'event_type']
    missing_cols = [col for col in required_cols if col not in first_chunk.columns]
    if missing_cols:
        raise ValueError(f"CSV missing required columns: {missing_cols}. Found columns: {first_chunk.columns.tolist()}")
    
    # Extract event types
    event_types.update(first_chunk['event_type'].unique())
    
    # Continue reading remaining chunks
    for chunk in chunk_iter:
        event_types.update(chunk['event_type'].unique())
    
    print(f"Found {len(event_types)} unique event types")
    
    # Insert event types
    db.set_search_path()
    for event_type_name in sorted(event_types):
        query = sql.SQL("""
            INSERT INTO churn_analytics.event_type (event_type_name)
            VALUES (%s)
            ON CONFLICT (event_type_name) DO NOTHING
        """)
        db.execute(query, (event_type_name,))
    db.commit()
    print("Event types inserted")
    
    # Create mapping dictionary
    query = sql.SQL("SELECT event_type_id, event_type_name FROM churn_analytics.event_type")
    db.execute(query)
    event_type_map = {row[1]: row[0] for row in db.cursor.fetchall()}
    print(f"Created event type mapping with {len(event_type_map)} types")
    
    # Now load events in batches
    print("Loading events...")
    total_rows = 0
    chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)
    
    for chunk_num, chunk in enumerate(chunk_iter):
        # Map event_type to event_type_id
        chunk['event_type_id'] = chunk['event_type'].map(event_type_map)
        
        # Prepare data for insertion
        # Convert account_id to string (in case it's read as numeric)
        chunk['account_id'] = chunk['account_id'].astype(str)
        
        # Convert event_time to datetime if needed
        chunk['event_time'] = pd.to_datetime(chunk['event_time'], errors='coerce')
        
        # Handle missing product_id and additional_data columns
        if 'product_id' not in chunk.columns:
            chunk['product_id'] = None
        else:
            # Convert product_id to string if present
            chunk['product_id'] = chunk['product_id'].astype(str).replace('nan', None)
        
        if 'additional_data' not in chunk.columns:
            chunk['additional_data'] = None
        
        # Filter out rows with invalid event_time or event_type_id
        chunk = chunk.dropna(subset=['event_time', 'event_type_id', 'account_id'])
        
        # Select columns in the right order
        insert_data = chunk[['account_id', 'event_time', 'event_type_id', 'product_id', 'additional_data']].values.tolist()
        
        # Insert in batches
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i+batch_size]
            query = sql.SQL("""
                INSERT INTO churn_analytics.event 
                (account_id, event_time, event_type_id, product_id, additional_data)
                VALUES %s
                ON CONFLICT (account_id, event_time, event_type_id) DO NOTHING
            """)
            execute_values(db.cursor, query, batch)
            db.commit()
            total_rows += len(batch)
            if total_rows % 100000 == 0:
                print(f"Loaded {total_rows:,} events...")
        
        print(f"Processed chunk {chunk_num + 1}, total rows: {total_rows:,}")
    
    print(f"Data loading complete! Total events loaded: {total_rows:,}")
    
    # Print summary statistics
    db.execute("SELECT COUNT(*) FROM churn_analytics.event")
    final_count = db.cursor.fetchone()[0]
    print(f"Final event count in database: {final_count:,}")
    
    db.execute("SELECT COUNT(*) FROM churn_analytics.event_type")
    type_count = db.cursor.fetchone()[0]
    print(f"Event types in database: {type_count}")


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


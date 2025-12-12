"""
Visualize event counts over time
"""
import pandas as pd
import matplotlib.pyplot as plt
from math import ceil
from pathlib import Path
try:
    from .database import Database
except ImportError:
    from database import Database
from psycopg2 import sql


def event_count_plot(event_data_path: str, event_name: str, output_dir: str = None):
    """
    Create a plot of event counts over time
    
    Args:
        event_data_path: Path to CSV file with event data (columns: event_date, n_event)
        event_name: Name of the event type for the plot title
        output_dir: Directory to save the plot (defaults to same directory as data)
    """
    qa_data_df = pd.read_csv(event_data_path)
    
    plt.figure(figsize=(12, 6))
    plt.plot('event_date', 'n_event', data=qa_data_df, marker='', color='black', linewidth=2)
    plt.ylim(0, ceil(1.1 * qa_data_df['n_event'].dropna().max()))
    plt.title(f'{event_name} event count over time')
    plt.xlabel('Date')
    plt.ylabel('Number of Events')
    plt.gca().figure.autofmt_xdate()
    
    # Show only first of each month on x-axis
    dates = qa_data_df['event_date'].tolist()
    plt.xticks(list(filter(lambda x: x.endswith(("01")), dates)), rotation=45)
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot
    if output_dir:
        output_path = Path(output_dir) / f'{event_name}_event_qa.png'
    else:
        output_path = Path(event_data_path).parent / f'{event_name}_event_qa.png'
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    print(f'Saving event QA plot to {output_path}')
    plt.close()


def get_events_per_day(db: Database, 
                       event_type_name: str,
                       start_date: str,
                       end_date: str,
                       output_path: str = None):
    """
    Query events per day from database and optionally save to CSV
    
    Args:
        db: Database instance
        event_type_name: Name of event type to query
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_path: Optional path to save CSV results
    
    Returns:
        DataFrame with event_date and n_event columns
    """
    # Read SQL query
    sql_path = Path(__file__).parent.parent / 'sql' / 'events_per_day.sql'
    with open(sql_path, 'r') as f:
        query = f.read()
    
    # Replace placeholders
    query = query.replace('%from_yyyy-mm-dd', start_date)
    query = query.replace('%to_yyyy-mm-dd', end_date)
    query = query.replace('%event2measure', event_type_name)
    
    db.set_search_path()
    db.execute(query)
    results = db.cursor.fetchall()
    
    # Convert to DataFrame
    df = pd.DataFrame(results, columns=['event_date', 'n_event'])
    
    if output_path:
        df.to_csv(output_path, index=False)
        print(f'Saved events per day data to {output_path}')
    
    return df


def visualize_event_from_db(db: Database,
                           event_type_name: str,
                           start_date: str,
                           end_date: str,
                           output_dir: str = None):
    """
    Query events per day from database and create visualization
    
    Args:
        db: Database instance
        event_type_name: Name of event type to visualize
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_dir: Directory to save outputs
    """
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f'{event_type_name}_events_per_day.csv'
    else:
        csv_path = Path(f'{event_type_name}_events_per_day.csv')
    
    # Get data from database
    df = get_events_per_day(db, event_type_name, start_date, end_date, str(csv_path))
    
    # Create visualization
    event_count_plot(str(csv_path), event_type_name, output_dir)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize event counts over time')
    parser.add_argument('event_name', type=str, help='Event type name to visualize')
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--csv-path', type=str, help='Path to CSV file (if not querying from DB)')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    parser.add_argument('--output-dir', type=str, help='Output directory for plots')
    
    args = parser.parse_args()
    
    if args.csv_path:
        # Visualize from CSV file
        event_count_plot(args.csv_path, args.event_name, args.output_dir)
    else:
        # Query from database and visualize
        db = Database(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
            schema=args.schema
        )
        try:
            visualize_event_from_db(db, args.event_name, args.start_date, 
                                   args.end_date, args.output_dir)
        finally:
            db.close()


"""
Run analysis queries and generate visualizations
Main script to run milestone 1 analyses
"""
import sys
import pandas as pd
from pathlib import Path
try:
    from .database import Database
    from .visualize_events import visualize_event_from_db, get_events_per_day
except ImportError:
    from database import Database
    from visualize_events import visualize_event_from_db, get_events_per_day
from psycopg2 import sql


def run_events_per_account_analysis(db: Database,
                                    start_date: str,
                                    end_date: str,
                                    output_path: str = None,
                                    min_events_per_month: float = 0.05):
    """
    Run events per account per month analysis
    
    Args:
        db: Database instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_path: Optional path to save CSV results
        min_events_per_month: Minimum events per month threshold
    
    Returns:
        DataFrame with analysis results
    """
    print(f"\n{'='*60}")
    print("Events Per Account Per Month Analysis")
    print(f"{'='*60}")
    
    # Read SQL query
    sql_path = Path(__file__).parent.parent / 'sql' / 'events_per_account.sql'
    with open(sql_path, 'r') as f:
        query = f.read()
    
    # Replace placeholders
    query = query.replace('%from_yyyy-mm-dd', start_date)
    query = query.replace('%to_yyyy-mm-dd', end_date)
    
    db.set_search_path()
    db.execute(query)
    results = db.cursor.fetchall()
    
    # Convert to DataFrame
    df = pd.DataFrame(results, columns=[
        'event_type_name', 'n_event', 'n_account', 'events_per_account',
        'n_months', 'events_per_account_per_month'
    ])
    
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"\nSaved results to {output_path}")
    
    # Print summary
    print(f"\nTotal event types: {len(df)}")
    print(f"\nMost common events (top 10):")
    print(df.head(10).to_string(index=False))
    
    print(f"\nLeast common events (bottom 10):")
    print(df.tail(10).to_string(index=False))
    
    # Count events with average > threshold
    above_threshold = df[df['events_per_account_per_month'] > min_events_per_month]
    print(f"\nEvents with average > {min_events_per_month} events per customer per month: {len(above_threshold)}")
    print(f"Percentage: {len(above_threshold)/len(df)*100:.1f}%")
    
    return df


def run_events_per_day_analysis(db: Database,
                                event_type_name: str,
                                start_date: str,
                                end_date: str,
                                output_dir: str = None):
    """
    Run events per day analysis and create visualization
    
    Args:
        db: Database instance
        event_type_name: Name of event type to analyze
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_dir: Directory to save outputs
    """
    print(f"\n{'='*60}")
    print(f"Events Per Day Analysis: {event_type_name}")
    print(f"{'='*60}")
    
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get data and create visualization
    visualize_event_from_db(db, event_type_name, start_date, end_date, output_dir)
    
    # Load the CSV to analyze patterns
    if output_dir:
        csv_path = output_dir / f'{event_type_name}_events_per_day.csv'
    else:
        csv_path = Path(f'{event_type_name}_events_per_day.csv')
    
    df = pd.read_csv(csv_path)
    df['event_date'] = pd.to_datetime(df['event_date'])
    
    # Analyze patterns
    print(f"\nDate range: {df['event_date'].min()} to {df['event_date'].max()}")
    print(f"Total days: {len(df)}")
    print(f"Days with events: {(df['n_event'] > 0).sum()}")
    print(f"Days with zero events: {(df['n_event'] == 0).sum()}")
    print(f"Average events per day: {df['n_event'].mean():.2f}")
    print(f"Median events per day: {df['n_event'].median():.2f}")
    print(f"Max events per day: {df['n_event'].max()}")
    print(f"Min events per day: {df['n_event'].min()}")
    
    # Check for gaps
    df_sorted = df.sort_values('event_date')
    date_diff = df_sorted['event_date'].diff().dt.days
    gaps = date_diff[date_diff > 1]
    if len(gaps) > 0:
        print(f"\nFound {len(gaps)} gaps in the data (more than 1 day between consecutive dates)")
    else:
        print("\nNo gaps found in the date sequence")
    
    # Check for outliers (using IQR method)
    Q1 = df['n_event'].quantile(0.25)
    Q3 = df['n_event'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = df[(df['n_event'] < lower_bound) | (df['n_event'] > upper_bound)]
    print(f"\nOutliers detected (using IQR method): {len(outliers)}")
    if len(outliers) > 0:
        print("Outlier dates:")
        print(outliers[['event_date', 'n_event']].to_string(index=False))


def main():
    """Main function to run all analyses"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run milestone 1 analyses')
    parser.add_argument('--start-date', type=str, required=True, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--event-type', type=str,
                       help='Event type name for per-day analysis (optional)')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory for results')
    parser.add_argument('--min-events-per-month', type=float, default=0.05,
                       help='Minimum events per month threshold')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    db = Database(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        schema=args.schema
    )
    
    try:
        # Run events per account analysis
        events_per_account_path = output_dir / 'events_per_account_per_month.csv'
        df_events = run_events_per_account_analysis(
            db, args.start_date, args.end_date,
            str(events_per_account_path),
            args.min_events_per_month
        )
        
        # If event type specified, run per-day analysis
        if args.event_type:
            run_events_per_day_analysis(
                db, args.event_type, args.start_date, args.end_date, str(output_dir)
            )
        else:
            # Use the most common event type
            most_common = df_events.iloc[0]['event_type_name']
            print(f"\n{'='*60}")
            print(f"Running per-day analysis for most common event: {most_common}")
            print(f"{'='*60}")
            run_events_per_day_analysis(
                db, most_common, args.start_date, args.end_date, str(output_dir)
            )
        
        print(f"\n{'='*60}")
        print("Analysis complete!")
        print(f"Results saved to: {output_dir}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error running analysis: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()


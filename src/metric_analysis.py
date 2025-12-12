"""
Analyze metrics: coverage, statistics over time, and visualizations
"""
import sys
import pandas as pd
from pathlib import Path
try:
    from .database import Database
except ImportError:
    from database import Database
from psycopg2 import sql


def calculate_metric_coverage(db: Database,
                            start_date: str,
                            end_date: str,
                            output_path: str = None):
    """
    Calculate metric coverage statistics
    
    Args:
        db: Database instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_path: Optional path to save CSV results
    
    Returns:
        DataFrame with coverage statistics
    """
    print(f"\n{'='*60}")
    print("Metric Coverage Analysis")
    print(f"{'='*60}")
    
    sql_path = Path(__file__).parent.parent / 'sql' / 'metric_coverage.sql'
    with open(sql_path, 'r') as f:
        query = f.read()
    
    query = query.replace('%from_yyyy-mm-dd', start_date)
    query = query.replace('%to_yyyy-mm-dd', end_date)
    
    db.set_search_path()
    db.execute(query)
    results = db.cursor.fetchall()
    
    df = pd.DataFrame(results, columns=[
        'metric_name', 'count_with_metric', 'n_account', 'pcnt_with_metric',
        'avg_value', 'min_value', 'max_value', 'earliest_metric', 'last_metric'
    ])
    
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"\nSaved coverage results to {output_path}")
    
    # Print summary
    print(f"\nTotal metrics: {len(df)}")
    print(f"\nMetric Coverage Summary:")
    print(df.to_string(index=False))
    
    return df


def get_metric_stats_over_time(db: Database,
                               metric_name: str,
                               start_date: str,
                               end_date: str,
                               output_path: str = None):
    """
    Get metric statistics over time
    
    Args:
        db: Database instance
        metric_name: Name of metric to analyze
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_path: Optional path to save CSV results
    
    Returns:
        DataFrame with stats over time
    """
    sql_path = Path(__file__).parent.parent / 'sql' / 'metric_stats_over_time.sql'
    with open(sql_path, 'r') as f:
        query = f.read()
    
    query = query.replace('%from_yyyy-mm-dd', start_date)
    query = query.replace('%to_yyyy-mm-dd', end_date)
    query = query.replace('%metric2measure', metric_name)
    
    db.set_search_path()
    db.execute(query)
    results = db.cursor.fetchall()
    
    df = pd.DataFrame(results, columns=['calc_date', 'avg', 'n_calc', 'min', 'max'])
    
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"Saved metric stats to {output_path}")
    
    return df


def visualize_metric_qa(qa_data_path: str, metric_name: str, output_dir: str = None):
    """
    Create metric QA visualization showing avg, min, max, and count over time
    
    Args:
        qa_data_path: Path to CSV file with metric stats
        metric_name: Name of metric for title
        output_dir: Directory to save plot
    """
    import matplotlib.pyplot as plt
    from math import ceil
    
    qa_data_df = pd.read_csv(qa_data_path)
    qa_data_df['calc_date'] = pd.to_datetime(qa_data_df['calc_date'])
    
    plt.figure(figsize=(12, 10))
    
    # Plot max
    plt.subplot(4, 1, 1)
    plt.plot('calc_date', 'max', data=qa_data_df, marker='', linestyle='-', 
             color='black', linewidth=2, label='max')
    plt.ylim(0, ceil(1.1 * qa_data_df['max'].dropna().max()) if qa_data_df['max'].notna().any() else 1)
    plt.legend()
    plt.ylabel('Max Value')
    
    # Plot avg
    plt.subplot(4, 1, 2)
    plt.plot('calc_date', 'avg', data=qa_data_df, marker='', linestyle='--', 
             color='black', linewidth=2, label='avg')
    plt.ylim(0, ceil(1.1 * qa_data_df['avg'].dropna().max()) if qa_data_df['avg'].notna().any() else 1)
    plt.legend()
    plt.ylabel('Average Value')
    
    # Plot min
    plt.subplot(4, 1, 3)
    plt.plot('calc_date', 'min', data=qa_data_df, marker='', linestyle='-.', 
             color='black', linewidth=2, label='min')
    plt.ylim(0, ceil(1.1 * qa_data_df['min'].dropna().max()) if qa_data_df['min'].notna().any() else 1)
    plt.legend()
    plt.ylabel('Min Value')
    
    # Plot count
    plt.subplot(4, 1, 4)
    plt.plot('calc_date', 'n_calc', data=qa_data_df, marker='', linestyle=':', 
             color='black', linewidth=2, label='n_calc')
    plt.ylim(0, ceil(1.1 * qa_data_df['n_calc'].dropna().max()) if qa_data_df['n_calc'].notna().any() else 1)
    plt.legend()
    plt.ylabel('Count')
    plt.xlabel('Date')
    
    plt.suptitle(f'{metric_name} Metric QA', fontsize=14, y=0.995)
    plt.gca().figure.autofmt_xdate()
    plt.tight_layout()
    
    if output_dir:
        output_path = Path(output_dir) / f'{metric_name}_metric_qa.png'
    else:
        output_path = Path(qa_data_path).parent / f'{metric_name}_metric_qa.png'
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    print(f'Saving metric QA plot to {output_path}')
    plt.close()


def analyze_metric_from_db(db: Database,
                          metric_name: str,
                          start_date: str,
                          end_date: str,
                          output_dir: str = None):
    """
    Get metric stats from database and create visualization
    
    Args:
        db: Database instance
        metric_name: Name of metric to analyze
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_dir: Directory to save outputs
    """
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f'{metric_name}_stats_over_time.csv'
    else:
        csv_path = Path(f'{metric_name}_stats_over_time.csv')
    
    # Get data from database
    df = get_metric_stats_over_time(db, metric_name, start_date, end_date, str(csv_path))
    
    # Create visualization
    visualize_metric_qa(str(csv_path), metric_name, output_dir)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Metric Stats Summary: {metric_name}")
    print(f"{'='*60}")
    print(f"Date range: {df['calc_date'].min()} to {df['calc_date'].max()}")
    print(f"Average value range: {df['avg'].min():.2f} to {df['avg'].max():.2f}")
    print(f"Max value range: {df['max'].min():.2f} to {df['max'].max():.2f}")
    print(f"Min value range: {df['min'].min():.2f} to {df['min'].max():.2f}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze metrics')
    parser.add_argument('--start-date', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--metric-name', type=str,
                       help='Metric name for stats over time analysis (optional)')
    parser.add_argument('--coverage-only', action='store_true',
                       help='Only calculate coverage, skip stats over time')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory for results')
    
    args = parser.parse_args()
    
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
        # Calculate coverage
        coverage_path = output_dir / 'metric_coverage.csv'
        calculate_metric_coverage(db, args.start_date, args.end_date, str(coverage_path))
        
        # Analyze specific metric if provided
        if args.metric_name and not args.coverage_only:
            analyze_metric_from_db(db, args.metric_name, args.start_date, 
                                  args.end_date, str(output_dir))
        elif not args.coverage_only:
            # Use first metric from coverage if available
            coverage_df = pd.read_csv(coverage_path)
            if len(coverage_df) > 0:
                first_metric = coverage_df.iloc[0]['metric_name']
                print(f"\n{'='*60}")
                print(f"Analyzing first metric: {first_metric}")
                print(f"{'='*60}")
                analyze_metric_from_db(db, first_metric, args.start_date,
                                      args.end_date, str(output_dir))
        
        print(f"\n{'='*60}")
        print("Analysis complete!")
        print(f"Results saved to: {output_dir}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error analyzing metrics: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


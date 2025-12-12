"""
Main script to run all metric calculations and analyses
Combines metric calculation and analysis steps
"""
import sys
from pathlib import Path
try:
    from .database import Database
    from .calculate_metrics import create_metric_tables, calculate_metrics_for_common_events
    from .metric_analysis import calculate_metric_coverage, analyze_metric_from_db
except ImportError:
    from database import Database
    from calculate_metrics import create_metric_tables, calculate_metrics_for_common_events
    from metric_analysis import calculate_metric_coverage, analyze_metric_from_db
import pandas as pd


def main():
    """Main function to run all metric calculations and analyses"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run milestone 2: Calculate and analyze metrics')
    parser.add_argument('--start-date', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--metric-name', type=str,
                       help='Specific metric name for detailed analysis (optional)')
    parser.add_argument('--min-events-per-month', type=float, default=0.05,
                       help='Minimum events per month threshold')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory for results')
    parser.add_argument('--skip-calculation', action='store_true',
                       help='Skip metric calculation (use existing metrics)')
    
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
        # Step 1: Create metric tables
        create_metric_tables(db)
        
        # Step 2: Calculate metrics (unless skipped)
        if not args.skip_calculation:
            calculate_metrics_for_common_events(
                db, args.start_date, args.end_date, args.min_events_per_month
            )
        else:
            print("Skipping metric calculation (using existing metrics)")
        
        # Step 3: Calculate metric coverage
        coverage_path = output_dir / 'metric_coverage.csv'
        coverage_df = calculate_metric_coverage(
            db, args.start_date, args.end_date, str(coverage_path)
        )
        
        # Step 4: Analyze specific metric or first metric
        if args.metric_name:
            metric_to_analyze = args.metric_name
        elif len(coverage_df) > 0:
            metric_to_analyze = coverage_df.iloc[0]['metric_name']
            print(f"\n{'='*60}")
            print(f"Analyzing first metric: {metric_to_analyze}")
            print(f"{'='*60}")
        else:
            print("\nNo metrics found to analyze")
            return
        
        analyze_metric_from_db(
            db, metric_to_analyze, args.start_date, args.end_date, str(output_dir)
        )
        
        print(f"\n{'='*60}")
        print("Milestone 2 Complete!")
        print(f"Results saved to: {output_dir}")
        print(f"\nDeliverables:")
        print(f"  1. Metric coverage: {coverage_path}")
        print(f"  2. Metric stats over time: {output_dir / f'{metric_to_analyze}_stats_over_time.csv'}")
        print(f"  3. Metric visualization: {output_dir / f'{metric_to_analyze}_metric_qa.png'}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error running metrics: {e}", file=sys.stderr)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()


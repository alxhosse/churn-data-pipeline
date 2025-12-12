"""
Main script to create current customer dataset and calculate statistics
Milestone 3: Current Customer Dataset
"""
import sys
import pandas as pd
from pathlib import Path
try:
    from .create_dataset import create_current_dataset
    from .dataset_stats import dataset_stats
except ImportError:
    from create_dataset import create_current_dataset
    from dataset_stats import dataset_stats


def main():
    """Main function to create dataset and calculate statistics"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run milestone 3: Create current customer dataset')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory for results')
    parser.add_argument('--dataset-name', type=str, default='current_customer_dataset.csv',
                       help='Name for the dataset file')
    parser.add_argument('--dbname', type=str, help='Database name')
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--schema', type=str, default='churn_analytics', help='Schema name')
    parser.add_argument('--skip-dataset', action='store_true',
                       help='Skip dataset creation (use existing dataset)')
    parser.add_argument('--dataset-path', type=str,
                       help='Path to existing dataset (if skipping creation)')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.skip_dataset and args.dataset_path:
        dataset_path = Path(args.dataset_path)
    else:
        dataset_path = output_dir / args.dataset_name
    
    # Step 1: Create dataset (unless skipped)
    if not args.skip_dataset:
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
            dataset = create_current_dataset(db, str(dataset_path))
            print(f"\nDataset created with {len(dataset):,} customers and {len(dataset.columns)} metrics")
        except Exception as e:
            print(f"Error creating dataset: {e}", file=sys.stderr)
            raise
        finally:
            db.close()
    else:
        print(f"Using existing dataset: {dataset_path}")
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {dataset_path}")
    
    # Step 2: Calculate statistics
    summary = dataset_stats(str(dataset_path))
    
    print(f"\n{'='*60}")
    print("Milestone 3 Complete!")
    print(f"{'='*60}")
    print(f"\nDeliverables:")
    print(f"  1. Current customer dataset: {dataset_path}")
    print(f"  2. Summary statistics: {dataset_path.parent / (dataset_path.stem + '_summarystats.csv')}")
    print(f"\nDataset contains:")
    print(f"  - {len(pd.read_csv(dataset_path, index_col=[0,1])):,} customers")
    print(f"  - {len(pd.read_csv(dataset_path, index_col=[0,1]).columns)} metrics")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()


"""
Calculate dataset statistics summary
"""
import sys
import pandas as pd
from pathlib import Path


def dataset_stats(data_set_path: str):
    """
    Calculate comprehensive statistics for a customer dataset
    
    Args:
        data_set_path: Path to CSV dataset file
    
    Returns:
        DataFrame with summary statistics
    """
    if not Path(data_set_path).is_file():
        raise FileNotFoundError(f'"{data_set_path}" is not a valid dataset path')
    
    print(f"\n{'='*60}")
    print("Calculating Dataset Statistics")
    print(f"{'='*60}")
    print(f"Loading dataset from: {data_set_path}")
    
    # Read dataset with account_id and last_metric_time as index
    churn_data = pd.read_csv(data_set_path, index_col=[0, 1])
    
    print(f"Dataset shape: {churn_data.shape[0]:,} rows × {churn_data.shape[1]} columns")
    
    # Handle is_churn column if present (for future use)
    if 'is_churn' in churn_data.columns:
        churn_data['is_churn'] = churn_data['is_churn'].astype(float)
    
    # Calculate basic statistics
    summary = churn_data.describe()
    summary = summary.transpose()
    
    # Add additional statistics
    summary['skew'] = churn_data.skew()
    summary['1%'] = churn_data.quantile(q=0.01)
    summary['99%'] = churn_data.quantile(q=0.99)
    
    # Calculate percentage of customers with nonzero values
    summary['nonzero'] = (churn_data.astype(bool).sum(axis=0) / churn_data.shape[0]) * 100
    
    # Reorder columns for standard format
    summary = summary[['count', 'nonzero', 'mean', 'std', 'skew', 'min', '1%', '25%', '50%', '75%', '99%', 'max']]
    summary.columns = summary.columns.str.replace("%", "pct")
    
    # Save results
    save_path = str(data_set_path).replace('.csv', '_summarystats.csv')
    summary.to_csv(save_path, header=True)
    print(f'\nSaved summary statistics to: {save_path}')
    
    # Print summary
    print(f"\n{'='*60}")
    print("Summary Statistics")
    print(f"{'='*60}")
    print(summary.to_string())
    
    # Answer key questions
    print(f"\n{'='*60}")
    print("Key Insights")
    print(f"{'='*60}")
    
    for col in churn_data.columns:
        nonzero_pct = summary.loc[col, 'nonzero']
        mean_val = summary.loc[col, 'mean']
        max_val = summary.loc[col, 'max']
        
        print(f"\n{col}:")
        print(f"  Customers engaged: {nonzero_pct:.1f}%")
        print(f"  Typical value (mean): {mean_val:.2f}")
        print(f"  Maximum value: {max_val:.2f}")
    
    return summary


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate dataset statistics')
    parser.add_argument('dataset_path', type=str,
                       help='Path to CSV dataset file')
    
    args = parser.parse_args()
    
    try:
        summary = dataset_stats(args.dataset_path)
        print(f"\n{'='*60}")
        print("Statistics calculation complete!")
        print(f"{'='*60}")
    except Exception as e:
        print(f"Error calculating statistics: {e}", file=sys.stderr)
        raise


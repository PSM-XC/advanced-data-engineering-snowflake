"""
Batch Load Orchestrator

Main script to orchestrate file splitting and batch loading into Snowflake.
This script handles 5 files with different batch sizes (250MB, 100MB, 150MB)
and compares loading performance using an XS data warehouse.
"""

import json
import argparse
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

from file_splitter import FileSplitter, split_multiple_files
from snowflake_loader import SnowflakeBatchLoader, compare_loading_performance


def load_config(config_file: str = None) -> dict:
    """
    Load configuration from file or return defaults.
    
    Args:
        config_file: Path to configuration JSON file
    
    Returns:
        Configuration dictionary
    """
    if config_file and Path(config_file).exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    
    # Default configuration
    return {
        "target_directory": "./batch_output",
        "warehouse_size": "XS",
        "file_configs": [
            {
                "source_file": "data/table1_data.csv",
                "batch_size_mb": 250,
                "table_name": "table1"
            },
            {
                "source_file": "data/table2_data.csv",
                "batch_size_mb": 100,
                "table_name": "table2"
            },
            {
                "source_file": "data/table3_data.csv",
                "batch_size_mb": 150,
                "table_name": "table3"
            },
            {
                "source_file": "data/table4_data.csv",
                "batch_size_mb": 250,
                "table_name": "table4"
            },
            {
                "source_file": "data/table5_data.csv",
                "batch_size_mb": 100,
                "table_name": "table5"
            }
        ],
        "snowflake_connection": {
            "account": "your_account",
            "user": "your_user",
            "password": "your_password",
            "database": "your_database",
            "schema": "your_schema",
            "warehouse": "your_warehouse"
        }
    }


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80 + "\n")


def print_summary_table(summaries: List):
    """Print a formatted summary table."""
    print("\n" + "-" * 80)
    print(f"{'Table':<15} {'Batch Size':<12} {'Batches':<10} {'Duration':<12} {'Throughput':<15}")
    print("-" * 80)
    
    for summary in summaries:
        print(f"{summary.table_name:<15} "
              f"{summary.batch_size_mb}MB{'':<8} "
              f"{summary.total_batches:<10} "
              f"{summary.total_duration_seconds:.2f}s{'':<7} "
              f"{summary.throughput_mb_per_second:.2f} MB/s")
    
    print("-" * 80)


def run_batch_loading_pipeline(
    config: dict,
    split_only: bool = False,
    load_only: bool = False
) -> dict:
    """
    Run the complete batch loading pipeline.
    
    Args:
        config: Configuration dictionary
        split_only: If True, only split files without loading
        load_only: If True, skip splitting and only load existing batches
    
    Returns:
        Dictionary with results and statistics
    """
    results = {
        'timestamp': datetime.now().isoformat(),
        'warehouse_size': config.get('warehouse_size', 'XS'),
        'split_results': {},
        'load_summaries': [],
        'comparison': {}
    }
    
    # Phase 1: Split files into batches
    if not load_only:
        print_header("Phase 1: Splitting Files into Batches")
        
        file_configs = [
            (fc['source_file'], fc['batch_size_mb'], fc['table_name'])
            for fc in config['file_configs']
        ]
        
        target_dir = config.get('target_directory', './batch_output')
        results['split_results'] = split_multiple_files(file_configs, target_dir)
        
        # Print split summary
        print("\nFile Splitting Summary:")
        for table_name, batch_files in results['split_results'].items():
            print(f"  {table_name}: {len(batch_files)} batches created")
    
    if split_only:
        print("\n✓ File splitting completed (load phase skipped)")
        return results
    
    # Phase 2: Load batches into Snowflake
    print_header("Phase 2: Loading Batches into Snowflake")
    
    snowflake_params = config.get('snowflake_connection', {})
    warehouse_size = config.get('warehouse_size', 'XS')
    
    loader = SnowflakeBatchLoader(snowflake_params, warehouse_size)
    
    try:
        loader.connect()
        
        # Load each table
        for fc in config['file_configs']:
            table_name = fc['table_name']
            batch_size_mb = fc['batch_size_mb']
            
            print(f"\nLoading {table_name} (batch size: {batch_size_mb}MB)...")
            
            if load_only:
                # Find existing batch files
                target_dir = Path(config.get('target_directory', './batch_output'))
                table_dir = target_dir / table_name
                batch_files = sorted(table_dir.glob(f"{table_name}_batch_*.csv"))
                batch_files = [str(f) for f in batch_files]
            else:
                batch_files = results['split_results'].get(table_name, [])
            
            if not batch_files:
                print(f"  ⚠ No batch files found for {table_name}")
                continue
            
            summary = loader.load_table_batches(
                table_name,
                batch_files,
                batch_size_mb
            )
            results['load_summaries'].append(summary)
        
        loader.disconnect()
        
    except Exception as e:
        print(f"\n✗ Error during loading: {str(e)}")
        loader.disconnect()
        return results
    
    # Phase 3: Compare performance
    print_header("Phase 3: Performance Comparison")
    
    if results['load_summaries']:
        results['comparison'] = compare_loading_performance(results['load_summaries'])
        
        print_summary_table(results['load_summaries'])
        
        print(f"\nOverall Statistics:")
        print(f"  Total Load Time: {results['comparison']['total_load_time']:.2f} seconds")
        print(f"  Fastest Table: {results['comparison']['fastest_table']}")
        print(f"  Highest Throughput: {results['comparison']['highest_throughput']}")
        
        # Analyze batch size impact
        print(f"\nBatch Size Analysis:")
        by_batch_size = {}
        for summary in results['load_summaries']:
            size = summary.batch_size_mb
            if size not in by_batch_size:
                by_batch_size[size] = {
                    'count': 0,
                    'avg_throughput': 0,
                    'total_throughput': 0
                }
            by_batch_size[size]['count'] += 1
            by_batch_size[size]['total_throughput'] += summary.throughput_mb_per_second
        
        for size, stats in sorted(by_batch_size.items()):
            avg_throughput = stats['total_throughput'] / stats['count']
            print(f"  {size}MB batches: Avg throughput = {avg_throughput:.2f} MB/s "
                  f"({stats['count']} table(s))")
    
    print_header("Pipeline Completed Successfully")
    
    return results


def save_results(results: dict, output_file: str):
    """
    Save results to a JSON file.
    
    Args:
        results: Results dictionary
        output_file: Path to output file
    """
    # Convert non-serializable objects
    serializable_results = {
        'timestamp': results['timestamp'],
        'warehouse_size': results['warehouse_size'],
        'split_results': {
            table: [str(f) for f in files]
            for table, files in results['split_results'].items()
        },
        'load_summaries': [
            {
                'table_name': s.table_name,
                'batch_size_mb': s.batch_size_mb,
                'total_batches': s.total_batches,
                'total_duration_seconds': s.total_duration_seconds,
                'total_rows_loaded': s.total_rows_loaded,
                'total_size_mb': s.total_size_mb,
                'avg_duration_per_batch': s.avg_duration_per_batch,
                'throughput_mb_per_second': s.throughput_mb_per_second
            }
            for s in results['load_summaries']
        ],
        'comparison': results['comparison']
    }
    
    with open(output_file, 'w') as f:
        json.dump(serializable_results, f, indent=2)
    
    print(f"\n✓ Results saved to: {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Split files and load into Snowflake in batches with performance comparison'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration JSON file'
    )
    parser.add_argument(
        '--split-only',
        action='store_true',
        help='Only split files, skip loading'
    )
    parser.add_argument(
        '--load-only',
        action='store_true',
        help='Only load existing batches, skip splitting'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='batch_load_results.json',
        help='Output file for results (default: batch_load_results.json)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Run pipeline
    results = run_batch_loading_pipeline(
        config,
        split_only=args.split_only,
        load_only=args.load_only
    )
    
    # Save results
    save_results(results, args.output)


if __name__ == '__main__':
    main()

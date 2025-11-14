#!/usr/bin/env python3
"""
Example Usage Script

This script demonstrates how to use the batch loading functionality
both as a standalone script and as importable modules.
"""

import sys
from pathlib import Path

# Add batch_loading to path if running from different directory
sys.path.insert(0, str(Path(__file__).parent))

from file_splitter import FileSplitter, split_multiple_files
from snowflake_loader import SnowflakeBatchLoader, compare_loading_performance


def example_1_basic_file_splitting():
    """Example 1: Basic file splitting."""
    print("\n" + "=" * 60)
    print("Example 1: Basic File Splitting")
    print("=" * 60)
    
    # Create a splitter
    splitter = FileSplitter(target_directory="./batch_output")
    
    # Split a single file
    source_file = "data/sample_data.csv"
    batch_size_mb = 250
    table_name = "my_table"
    
    print(f"\nSplitting {source_file} into {batch_size_mb}MB batches...")
    
    # Note: This will fail if the file doesn't exist
    try:
        batch_files = splitter.split_file_by_lines(
            source_file,
            batch_size_mb,
            table_name
        )
        
        print(f"Created {len(batch_files)} batch files:")
        for batch_file in batch_files:
            size_mb = splitter.get_file_size_mb(batch_file)
            print(f"  - {batch_file} ({size_mb:.2f} MB)")
        
        # Get summary
        summary = splitter.create_batch_summary(batch_files)
        print(f"\nTotal size: {summary['total_size_mb']} MB")
        
    except FileNotFoundError as e:
        print(f"  ⚠ {e}")
        print(f"  (This is expected if sample data doesn't exist)")


def example_2_multiple_files():
    """Example 2: Split multiple files with different batch sizes."""
    print("\n" + "=" * 60)
    print("Example 2: Split Multiple Files")
    print("=" * 60)
    
    # Define configurations for 5 tables with different batch sizes
    file_configs = [
        ("data/table1_data.csv", 250, "table1"),  # 250MB batches
        ("data/table2_data.csv", 100, "table2"),  # 100MB batches
        ("data/table3_data.csv", 150, "table3"),  # 150MB batches
        ("data/table4_data.csv", 250, "table4"),  # 250MB batches
        ("data/table5_data.csv", 100, "table5"),  # 100MB batches
    ]
    
    target_directory = "./batch_output"
    
    print(f"\nSplitting 5 files with varying batch sizes...")
    results = split_multiple_files(file_configs, target_directory)
    
    print("\nResults:")
    for table_name, batch_files in results.items():
        print(f"  {table_name}: {len(batch_files)} batches")


def example_3_snowflake_loading():
    """Example 3: Load batches into Snowflake (simulated)."""
    print("\n" + "=" * 60)
    print("Example 3: Snowflake Batch Loading (Simulated)")
    print("=" * 60)
    
    # Connection parameters
    connection_params = {
        "account": "your_account",
        "user": "your_user",
        "password": "your_password",
        "database": "your_database",
        "schema": "your_schema"
    }
    
    # Create loader with XS warehouse
    loader = SnowflakeBatchLoader(
        connection_params,
        warehouse_size="XS"
    )
    
    # Connect
    loader.connect()
    
    # Simulate loading a table's batches
    table_name = "table1"
    batch_files = [
        f"./batch_output/{table_name}/{table_name}_batch_1.csv",
        f"./batch_output/{table_name}/{table_name}_batch_2.csv",
    ]
    batch_size_mb = 250
    
    print(f"\nLoading batches for {table_name}...")
    
    # Note: This will simulate the loading process
    try:
        summary = loader.load_table_batches(
            table_name,
            batch_files,
            batch_size_mb
        )
        
        print(f"\nLoading Summary:")
        print(f"  Total batches: {summary.total_batches}")
        print(f"  Total duration: {summary.total_duration_seconds:.2f}s")
        print(f"  Throughput: {summary.throughput_mb_per_second:.2f} MB/s")
        
    except Exception as e:
        print(f"  ⚠ {e}")
    
    loader.disconnect()


def example_4_performance_comparison():
    """Example 4: Compare performance across multiple tables."""
    print("\n" + "=" * 60)
    print("Example 4: Performance Comparison")
    print("=" * 60)
    
    from snowflake_loader import TableLoadSummary
    
    # Create sample summaries (in real use, these come from actual loads)
    summaries = [
        TableLoadSummary(
            table_name="table1",
            batch_size_mb=250,
            total_batches=4,
            total_duration_seconds=120.5,
            total_rows_loaded=1000000,
            total_size_mb=1000.0,
            avg_duration_per_batch=30.125,
            throughput_mb_per_second=8.3
        ),
        TableLoadSummary(
            table_name="table2",
            batch_size_mb=100,
            total_batches=10,
            total_duration_seconds=135.2,
            total_rows_loaded=1000000,
            total_size_mb=1000.0,
            avg_duration_per_batch=13.52,
            throughput_mb_per_second=7.4
        ),
        TableLoadSummary(
            table_name="table3",
            batch_size_mb=150,
            total_batches=7,
            total_duration_seconds=98.7,
            total_rows_loaded=1050000,
            total_size_mb=1050.0,
            avg_duration_per_batch=14.1,
            throughput_mb_per_second=10.6
        ),
    ]
    
    # Compare performance
    comparison = compare_loading_performance(summaries)
    
    print("\nPerformance Comparison:")
    print(f"  Total load time: {comparison['total_load_time']:.2f}s")
    print(f"  Fastest table: {comparison['fastest_table']}")
    print(f"  Highest throughput: {comparison['highest_throughput']}")
    
    print("\nDetailed Table Stats:")
    for table_info in comparison['tables']:
        print(f"\n  {table_info['table_name']}:")
        print(f"    Batch size: {table_info['batch_size_mb']}MB")
        print(f"    Total batches: {table_info['total_batches']}")
        print(f"    Duration: {table_info['total_duration_seconds']:.2f}s")
        print(f"    Throughput: {table_info['throughput_mb_per_second']:.2f} MB/s")


def example_5_complete_workflow():
    """Example 5: Complete workflow from splitting to loading."""
    print("\n" + "=" * 60)
    print("Example 5: Complete Workflow")
    print("=" * 60)
    
    print("\nThis is the typical workflow:")
    print("\n1. Split files into batches")
    print("   -> Use FileSplitter or split_multiple_files()")
    
    print("\n2. Connect to Snowflake")
    print("   -> Create SnowflakeBatchLoader with connection params")
    
    print("\n3. Load each table's batches")
    print("   -> Use loader.load_table_batches() for each table")
    
    print("\n4. Compare performance")
    print("   -> Use compare_loading_performance() with all summaries")
    
    print("\n5. Analyze results")
    print("   -> Determine optimal batch size for your use case")
    
    print("\n✓ See batch_load_orchestrator.py for complete implementation")


def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print(" Batch Loading Examples")
    print("=" * 60)
    
    # Run examples
    example_1_basic_file_splitting()
    example_2_multiple_files()
    example_3_snowflake_loading()
    example_4_performance_comparison()
    example_5_complete_workflow()
    
    print("\n" + "=" * 60)
    print(" Examples Complete")
    print("=" * 60)
    print("\nFor production use:")
    print("  1. Generate test data: python generate_sample_data.py")
    print("  2. Run pipeline: python batch_load_orchestrator.py --config config.json")
    print("\n")


if __name__ == "__main__":
    main()

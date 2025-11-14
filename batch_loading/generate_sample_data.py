"""
Sample Data Generator

Generate sample CSV files for testing the batch loading functionality.
"""

import csv
import random
import argparse
from pathlib import Path
from datetime import datetime, timedelta


def generate_csv_file(
    output_file: str,
    target_size_mb: float,
    num_columns: int = 10
) -> int:
    """
    Generate a CSV file with random data up to the target size.
    
    Args:
        output_file: Path to the output CSV file
        target_size_mb: Target size in megabytes
        num_columns: Number of columns to generate
    
    Returns:
        Number of rows generated
    """
    target_size_bytes = int(target_size_mb * 1024 * 1024)
    
    # Create output directory if needed
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Define column headers
    headers = [f'column_{i}' for i in range(1, num_columns + 1)]
    
    # Generate sample data
    rows_written = 0
    current_size = 0
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(headers)
        current_size += len(','.join(headers).encode('utf-8')) + 1
        
        # Generate rows until target size is reached
        while current_size < target_size_bytes:
            row = generate_sample_row(num_columns)
            writer.writerow(row)
            
            # Estimate row size
            row_str = ','.join(str(x) for x in row)
            current_size += len(row_str.encode('utf-8')) + 1
            rows_written += 1
            
            # Print progress every 10000 rows
            if rows_written % 10000 == 0:
                progress_mb = current_size / (1024 * 1024)
                print(f"  Progress: {progress_mb:.2f} MB / {target_size_mb:.2f} MB "
                      f"({rows_written:,} rows)")
    
    final_size_mb = current_size / (1024 * 1024)
    print(f"  ✓ Generated: {output_file}")
    print(f"    Size: {final_size_mb:.2f} MB")
    print(f"    Rows: {rows_written:,}")
    
    return rows_written


def generate_sample_row(num_columns: int) -> list:
    """
    Generate a single row of sample data.
    
    Args:
        num_columns: Number of columns to generate
    
    Returns:
        List of column values
    """
    row = []
    
    for i in range(num_columns):
        # Vary data types across columns
        col_type = i % 5
        
        if col_type == 0:  # Integer ID
            row.append(random.randint(1, 1000000))
        elif col_type == 1:  # Float value
            row.append(round(random.uniform(0, 10000), 2))
        elif col_type == 2:  # Date
            base_date = datetime(2020, 1, 1)
            random_days = random.randint(0, 1460)  # ~4 years
            date = base_date + timedelta(days=random_days)
            row.append(date.strftime('%Y-%m-%d'))
        elif col_type == 3:  # Category
            categories = ['Category_A', 'Category_B', 'Category_C', 'Category_D']
            row.append(random.choice(categories))
        else:  # Text description
            text_options = [
                'Sample text data for testing purposes',
                'Another example of text content',
                'This is a longer description field with more content',
                'Short text',
                'Medium length text for CSV testing'
            ]
            row.append(random.choice(text_options))
    
    return row


def generate_test_dataset(
    output_directory: str = 'data',
    file_configs: list = None
):
    """
    Generate a complete test dataset with 5 files.
    
    Args:
        output_directory: Directory to create data files in
        file_configs: List of (filename, size_mb) tuples
    """
    if file_configs is None:
        # Default: Generate 5 files with varying sizes
        file_configs = [
            ('table1_data.csv', 300),  # Will be split into 250MB batches
            ('table2_data.csv', 350),  # Will be split into 100MB batches
            ('table3_data.csv', 450),  # Will be split into 150MB batches
            ('table4_data.csv', 500),  # Will be split into 250MB batches
            ('table5_data.csv', 250),  # Will be split into 100MB batches
        ]
    
    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"\nGenerating test dataset in: {output_directory}")
    print("=" * 60)
    
    total_rows = 0
    for filename, size_mb in file_configs:
        print(f"\nGenerating {filename} (target: {size_mb} MB)...")
        file_path = output_path / filename
        rows = generate_csv_file(str(file_path), size_mb)
        total_rows += rows
    
    print("\n" + "=" * 60)
    print(f"Dataset generation complete!")
    print(f"Total files: {len(file_configs)}")
    print(f"Total rows: {total_rows:,}")
    print(f"Output directory: {output_directory}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate sample CSV files for batch loading testing'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data',
        help='Output directory for generated files (default: data)'
    )
    parser.add_argument(
        '--small',
        action='store_true',
        help='Generate smaller files for quick testing (10MB each)'
    )
    
    args = parser.parse_args()
    
    if args.small:
        # Generate small test files
        file_configs = [
            ('table1_data.csv', 10),
            ('table2_data.csv', 10),
            ('table3_data.csv', 10),
            ('table4_data.csv', 10),
            ('table5_data.csv', 10),
        ]
        print("\n⚠ Generating SMALL test files (10MB each)")
    else:
        # Generate full-size test files
        file_configs = None  # Use defaults
        print("\n⚠ Generating FULL-SIZE test files (300-500MB each)")
        print("This may take several minutes...")
    
    generate_test_dataset(args.output_dir, file_configs)
    
    print("\n✓ You can now run the batch loading pipeline:")
    print(f"  python batch_load_orchestrator.py --config config_example.json --split-only")


if __name__ == '__main__':
    main()

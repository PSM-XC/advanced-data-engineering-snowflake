# Batch File Splitter and Snowflake Loader

This module provides functionality to split large files into batches and load them into Snowflake tables with performance measurement and comparison.

## Features

- **File Splitting**: Split large CSV files into smaller batches (250MB, 100MB, 150MB)
- **Batch Loading**: Load split files into Snowflake tables using configurable warehouse sizes
- **Performance Measurement**: Track loading times, throughput, and compare performance across different batch sizes
- **XS Warehouse Support**: Optimized for testing with Snowflake XS data warehouses

## Components

### 1. file_splitter.py
Handles splitting large files into smaller batches:
- Supports both binary and line-aware (CSV) splitting
- Maintains CSV headers in each batch file
- Generates organized directory structure per table
- Provides file size summaries

### 2. snowflake_loader.py
Manages batch loading into Snowflake:
- Connects to Snowflake with configurable parameters
- Creates stages and tables
- Loads batch files with timing measurements
- Generates performance summaries and comparisons

### 3. batch_load_orchestrator.py
Main orchestration script:
- Coordinates file splitting and loading phases
- Supports configuration files
- Generates performance comparison reports
- Provides CLI interface for flexible execution

## Installation

### Prerequisites
- Python 3.8 or higher
- Snowflake account (for actual loading)

### Required Python Packages
```bash
pip install snowflake-connector-python
```

For simulation/testing (without Snowflake):
```bash
# No additional packages required
```

## Usage

### Basic Usage

1. **Split files only** (no Snowflake connection needed):
```bash
python batch_load_orchestrator.py --config config_example.json --split-only
```

2. **Split and load into Snowflake**:
```bash
python batch_load_orchestrator.py --config config_example.json
```

3. **Load existing batches** (skip splitting):
```bash
python batch_load_orchestrator.py --config config_example.json --load-only
```

### Configuration File

Create a JSON configuration file (see `config_example.json`):

```json
{
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
    ...
  ],
  "snowflake_connection": {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "database": "BATCH_LOAD_DB",
    "schema": "PUBLIC",
    "warehouse": "BATCH_LOAD_WH"
  }
}
```

### Command Line Arguments

- `--config <file>`: Path to configuration JSON file
- `--split-only`: Only split files without loading to Snowflake
- `--load-only`: Skip splitting and only load existing batch files
- `--output <file>`: Output file for results (default: batch_load_results.json)

## Example Workflow

### 1. Prepare Your Data Files

Place your CSV files in a data directory:
```
data/
  ├── table1_data.csv
  ├── table2_data.csv
  ├── table3_data.csv
  ├── table4_data.csv
  └── table5_data.csv
```

### 2. Configure Your Settings

Edit `config_example.json` with your:
- File paths
- Batch sizes (250MB, 100MB, 150MB)
- Table names
- Snowflake connection details

### 3. Run the Pipeline

```bash
# Split files and load to Snowflake
python batch_load_orchestrator.py --config my_config.json

# Or just split files for testing
python batch_load_orchestrator.py --config my_config.json --split-only
```

### 4. Review Results

The script generates:
- Batch files in the target directory (organized by table)
- Console output with performance metrics
- JSON results file with detailed statistics

Example output structure:
```
batch_output/
  ├── table1/
  │   ├── table1_batch_1.csv
  │   ├── table1_batch_2.csv
  │   └── ...
  ├── table2/
  │   ├── table2_batch_1.csv
  │   └── ...
  └── ...
```

## Performance Comparison

The script automatically compares:
- **Loading time** for each table
- **Throughput** (MB/s) for different batch sizes
- **Average duration** per batch
- **Total rows loaded**

Example output:
```
================================================================================
 Phase 3: Performance Comparison
================================================================================

--------------------------------------------------------------------------------
Table           Batch Size   Batches    Duration    Throughput     
--------------------------------------------------------------------------------
table1          250MB        4          120.45s     52.31 MB/s
table2          100MB        10         135.20s     48.92 MB/s
table3          150MB        7          98.75s      55.18 MB/s
table4          250MB        5          145.30s     50.45 MB/s
table5          100MB        8          112.40s     51.23 MB/s
--------------------------------------------------------------------------------

Overall Statistics:
  Total Load Time: 612.10 seconds
  Fastest Table: table3
  Highest Throughput: table3

Batch Size Analysis:
  100MB batches: Avg throughput = 50.08 MB/s (2 table(s))
  150MB batches: Avg throughput = 55.18 MB/s (1 table(s))
  250MB batches: Avg throughput = 51.38 MB/s (2 table(s))
```

## Use as Python Module

You can also use the components as Python modules:

```python
from file_splitter import FileSplitter, split_multiple_files
from snowflake_loader import SnowflakeBatchLoader

# Split files
file_configs = [
    ("data/file1.csv", 250, "table1"),
    ("data/file2.csv", 100, "table2"),
]
batch_results = split_multiple_files(file_configs, "./output")

# Load to Snowflake
connection_params = {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
}
loader = SnowflakeBatchLoader(connection_params, warehouse_size="XS")
loader.connect()

for table_name, batch_files in batch_results.items():
    summary = loader.load_table_batches(table_name, batch_files, 250)
    print(f"{table_name}: {summary.total_duration_seconds}s")
```

## Testing

To test without Snowflake:
1. Use `--split-only` flag to test file splitting
2. The loader includes simulation mode for testing logic

```bash
# Test file splitting
python batch_load_orchestrator.py --config config_example.json --split-only

# Test with simulated loading (no actual Snowflake connection)
python batch_load_orchestrator.py --config config_example.json
```

## Notes

- **CSV Headers**: The splitter preserves CSV headers in each batch file
- **File Encoding**: UTF-8 encoding is used by default
- **Error Handling**: Failed batches are logged but don't stop the pipeline
- **Stage Creation**: Temporary stages are created per table
- **Warehouse Size**: XS warehouse is recommended for testing; adjust for production

## Troubleshooting

### File Not Found
- Ensure source files exist in the paths specified in config
- Use absolute paths or paths relative to script execution directory

### Connection Errors
- Verify Snowflake credentials in configuration
- Check network connectivity
- Ensure warehouse is running

### Memory Issues
- For very large files, the line-aware splitter reads one line at a time
- Batch sizes can be adjusted based on available memory

## Future Enhancements

Potential improvements:
- Parallel batch loading
- Compression support
- Multiple file formats (JSON, Parquet)
- Advanced error recovery
- Real-time progress monitoring
- Database schema inference

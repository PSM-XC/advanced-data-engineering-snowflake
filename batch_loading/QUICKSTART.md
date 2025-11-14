# Quick Start Guide

Get started with batch file splitting and Snowflake loading in 5 minutes.

## Prerequisites

- Python 3.8 or higher
- (Optional) Snowflake account for actual loading

## Step 1: Generate Test Data

Generate small test files (10MB each) for testing:

```bash
cd batch_loading
python3 generate_sample_data.py --small
```

This creates 5 CSV files in the `data/` directory.

## Step 2: Test File Splitting

Split the files into batches without connecting to Snowflake:

```bash
python3 batch_load_orchestrator.py --config config_example.json --split-only
```

This will:
- Split each file into the configured batch sizes
- Create organized directories in `batch_output/`
- Display a summary of created batches

**Expected Output:**
```
================================================================================
 Phase 1: Splitting Files into Batches
================================================================================

Processing table1: splitting data/table1_data.csv into 250MB batches...
  Created 1 batches (Total: 10.0MB)
...
```

## Step 3: Review the Batch Files

Check the created batch files:

```bash
ls -lh batch_output/*/
```

You should see organized directories like:
```
batch_output/table1/
  table1_batch_1.csv
batch_output/table2/
  table2_batch_1.csv
  table2_batch_2.csv
...
```

## Step 4: (Optional) Full Pipeline with Snowflake

If you have Snowflake credentials, update `config_example.json` with your connection details:

```json
{
  "snowflake_connection": {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "database": "your_database",
    "schema": "your_schema",
    "warehouse": "your_warehouse"
  }
}
```

Then run the full pipeline:

```bash
python3 batch_load_orchestrator.py --config config_example.json
```

This will split files AND load them into Snowflake with performance measurement.

## Understanding the Results

After running, you'll get:

1. **Console Output** - Shows progress, timing, and performance comparison
2. **JSON Results File** - Contains detailed metrics (default: `batch_load_results.json`)
3. **Batch Files** - Organized in `batch_output/` directory

### Performance Comparison Output

```
--------------------------------------------------------------------------------
Table           Batch Size   Batches    Duration     Throughput     
--------------------------------------------------------------------------------
table1          250MB        4          120.45s      52.31 MB/s
table2          100MB        10         135.20s      48.92 MB/s
table3          150MB        7          98.75s       55.18 MB/s
...
```

**Key Metrics:**
- **Duration**: Total time to load all batches for that table
- **Throughput**: MB/s loading speed
- **Batch Size Analysis**: Compare average throughput for different batch sizes

## Common Use Cases

### 1. Generate Full-Size Test Data

For realistic testing with larger files (300-500MB each):

```bash
python3 generate_sample_data.py
# Warning: This takes several minutes
```

### 2. Custom Configuration

Create your own config file:

```bash
cp config_example.json my_config.json
# Edit my_config.json with your settings
python3 batch_load_orchestrator.py --config my_config.json --split-only
```

### 3. Only Load Existing Batches

If you already split files and just want to load:

```bash
python3 batch_load_orchestrator.py --config config_example.json --load-only
```

### 4. Use as Python Module

```python
from file_splitter import split_multiple_files

file_configs = [
    ("data/file1.csv", 250, "table1"),
    ("data/file2.csv", 100, "table2"),
]
results = split_multiple_files(file_configs, "./output")
```

## Troubleshooting

### "Source file not found"
- Run `generate_sample_data.py` first
- Check that paths in config file are correct
- Use absolute paths if relative paths don't work

### "Connection error"
- Verify Snowflake credentials in config
- Test connection with `snowflake-connector-python`
- Check if warehouse is running

### Memory issues with large files
- The splitter reads line-by-line, so it's memory efficient
- Reduce batch size if needed
- Process files one at a time

## Next Steps

1. **Read the full README.md** for detailed documentation
2. **Check example_usage.py** for code examples
3. **Adjust batch sizes** based on your performance tests
4. **Integrate with your data pipeline**

## Support

For issues or questions, refer to:
- Full documentation: `README.md`
- Example code: `example_usage.py`
- Repository: [GitHub Issues](https://github.com/PSM-XC/advanced-data-engineering-snowflake/issues)

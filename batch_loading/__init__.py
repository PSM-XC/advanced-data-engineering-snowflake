"""
Batch Loading Package

This package provides tools for splitting large files into batches
and loading them into Snowflake with performance measurement.
"""

from .file_splitter import FileSplitter, split_multiple_files
from .snowflake_loader import (
    SnowflakeBatchLoader,
    LoadResult,
    TableLoadSummary,
    compare_loading_performance
)

__version__ = "1.0.0"
__all__ = [
    "FileSplitter",
    "split_multiple_files",
    "SnowflakeBatchLoader",
    "LoadResult",
    "TableLoadSummary",
    "compare_loading_performance"
]

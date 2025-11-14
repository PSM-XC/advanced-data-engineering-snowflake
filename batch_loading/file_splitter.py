"""
File Splitter Module for Batch Loading

This module provides functionality to split large files into smaller batches
for efficient loading into Snowflake tables.
"""

import os
import shutil
from typing import List, Tuple
from pathlib import Path


class FileSplitter:
    """
    A class to split files into batches of specified sizes.
    """
    
    def __init__(self, target_directory: str):
        """
        Initialize the FileSplitter.
        
        Args:
            target_directory: Directory where batch files will be created
        """
        self.target_directory = Path(target_directory)
        self.target_directory.mkdir(parents=True, exist_ok=True)
    
    def split_file(
        self, 
        source_file: str, 
        batch_size_mb: int, 
        table_name: str
    ) -> List[str]:
        """
        Split a file into batches of specified size.
        
        Args:
            source_file: Path to the source file to split
            batch_size_mb: Size of each batch in megabytes
            table_name: Name of the table (used for naming batch files)
        
        Returns:
            List of paths to the created batch files
        """
        source_path = Path(source_file)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        batch_size_bytes = batch_size_mb * 1024 * 1024
        batch_files = []
        
        # Create table-specific directory
        table_dir = self.target_directory / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        
        with open(source_path, 'rb') as source:
            batch_number = 1
            
            while True:
                # Read batch_size_bytes from source
                chunk = source.read(batch_size_bytes)
                
                if not chunk:
                    break
                
                # Create batch file
                batch_filename = f"{table_name}_batch_{batch_number}.csv"
                batch_path = table_dir / batch_filename
                
                with open(batch_path, 'wb') as batch_file:
                    batch_file.write(chunk)
                
                batch_files.append(str(batch_path))
                batch_number += 1
        
        return batch_files
    
    def split_file_by_lines(
        self, 
        source_file: str, 
        batch_size_mb: int, 
        table_name: str
    ) -> List[str]:
        """
        Split a text file into batches by complete lines (CSV-aware).
        
        This method ensures that lines are not split in the middle,
        which is important for CSV files.
        
        Args:
            source_file: Path to the source file to split
            batch_size_mb: Target size of each batch in megabytes
            table_name: Name of the table (used for naming batch files)
        
        Returns:
            List of paths to the created batch files
        """
        source_path = Path(source_file)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        batch_size_bytes = batch_size_mb * 1024 * 1024
        batch_files = []
        
        # Create table-specific directory
        table_dir = self.target_directory / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        
        with open(source_path, 'r', encoding='utf-8') as source:
            batch_number = 1
            current_batch_size = 0
            batch_lines = []
            header = None
            
            for line_number, line in enumerate(source):
                # Save header from first line
                if line_number == 0:
                    header = line
                    continue
                
                line_size = len(line.encode('utf-8'))
                
                # Check if adding this line would exceed batch size
                if current_batch_size + line_size > batch_size_bytes and batch_lines:
                    # Write current batch
                    batch_filename = f"{table_name}_batch_{batch_number}.csv"
                    batch_path = table_dir / batch_filename
                    
                    with open(batch_path, 'w', encoding='utf-8') as batch_file:
                        if header:
                            batch_file.write(header)
                        batch_file.writelines(batch_lines)
                    
                    batch_files.append(str(batch_path))
                    batch_number += 1
                    
                    # Reset for next batch
                    batch_lines = []
                    current_batch_size = 0
                
                # Add line to current batch
                batch_lines.append(line)
                current_batch_size += line_size
            
            # Write remaining lines if any
            if batch_lines:
                batch_filename = f"{table_name}_batch_{batch_number}.csv"
                batch_path = table_dir / batch_filename
                
                with open(batch_path, 'w', encoding='utf-8') as batch_file:
                    if header:
                        batch_file.write(header)
                    batch_file.writelines(batch_lines)
                
                batch_files.append(str(batch_path))
        
        return batch_files
    
    def get_file_size_mb(self, file_path: str) -> float:
        """
        Get the size of a file in megabytes.
        
        Args:
            file_path: Path to the file
        
        Returns:
            File size in megabytes
        """
        return Path(file_path).stat().st_size / (1024 * 1024)
    
    def create_batch_summary(self, batch_files: List[str]) -> dict:
        """
        Create a summary of batch files.
        
        Args:
            batch_files: List of batch file paths
        
        Returns:
            Dictionary with batch summary information
        """
        summary = {
            'total_batches': len(batch_files),
            'batch_details': []
        }
        
        for batch_file in batch_files:
            size_mb = self.get_file_size_mb(batch_file)
            summary['batch_details'].append({
                'file': batch_file,
                'size_mb': round(size_mb, 2)
            })
        
        total_size = sum(detail['size_mb'] for detail in summary['batch_details'])
        summary['total_size_mb'] = round(total_size, 2)
        
        return summary


def split_multiple_files(
    file_configs: List[Tuple[str, int, str]],
    target_directory: str
) -> dict:
    """
    Split multiple files with different batch sizes.
    
    Args:
        file_configs: List of tuples (source_file, batch_size_mb, table_name)
        target_directory: Directory where batch files will be created
    
    Returns:
        Dictionary mapping table names to their batch file lists
    """
    splitter = FileSplitter(target_directory)
    results = {}
    
    for source_file, batch_size_mb, table_name in file_configs:
        print(f"Processing {table_name}: splitting {source_file} into {batch_size_mb}MB batches...")
        
        try:
            batch_files = splitter.split_file_by_lines(
                source_file, 
                batch_size_mb, 
                table_name
            )
            results[table_name] = batch_files
            
            summary = splitter.create_batch_summary(batch_files)
            print(f"  Created {summary['total_batches']} batches "
                  f"(Total: {summary['total_size_mb']}MB)")
        except Exception as e:
            print(f"  Error processing {table_name}: {str(e)}")
            results[table_name] = []
    
    return results

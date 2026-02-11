"""
Extract module - Extract data from Excel files.

This module handles reading Excel files from Fraga and preparing
the data for loading into staging.
"""

import pandas as pd
from typing import List
from pathlib import Path


def read_excel_file(
    file_path: str,
) -> pd.DataFrame:
    """
    Read Excel file and return as DataFrame.
    
    Args:
        file_path: Path to Excel file
        
    Returns:
        DataFrame with raw data from Excel
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    file_path_obj = Path(file_path)
    
    # Validate file exists
    if not file_path_obj.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")
    
    # Validate file extension
    if file_path_obj.suffix.lower() not in ['.xlsx', '.xls']:
        raise ValueError(f"Invalid file format. Expected .xlsx or .xls, got: {file_path_obj.suffix}")
    
    try:
        # Read Excel file
        # If sheet_name is None, read first sheet (same as analyze_excel.py)
        # header=0 means first row is header (auto-detected by pandas)
        df = pd.read_excel(
            file_path,
            header=0  # First row is header (auto-detected)
        )
        
        return df
        
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")


def validate_excel_structure(df: pd.DataFrame) -> bool:
    """
    Validate that Excel has expected columns.
    
    Args:
        df: DataFrame from Excel
        
    Returns:
        True if structure is valid
        
    Raises:
        ValueError: If required columns are missing
    """
    if df.empty:
        raise ValueError("Excel file is empty")
    
    # Required columns based on analysis
    # These are the minimum columns we expect
    required_columns = [
        'Código Fraga',
        'Marca',
        'Nome Veículo',
        'Modelo Veículo'
    ]
    
    # Check for required columns (case-insensitive)
    df_columns_lower = [col.lower() for col in df.columns]
    missing_columns = []
    
    for req_col in required_columns:
        if req_col.lower() not in df_columns_lower:
            missing_columns.append(req_col)
    
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {', '.join(missing_columns)}. "
            f"Found columns: {', '.join(df.columns.tolist())}"
        )
    
    return True


def list_excel_files(directory: str) -> List[str]:
    """
    List all Excel files in directory.
    
    Args:
        directory: Directory to search
        
    Returns:
        List of Excel file paths (absolute paths)
    """
    dir_path = Path(directory)
    
    if not dir_path.exists():
        return []
    
    # Find all Excel files
    excel_files = []
    for ext in ['*.xlsx', '*.xls']:
        excel_files.extend(dir_path.glob(ext))
    
    # Return as list of absolute paths (as strings)
    return [str(f.absolute()) for f in excel_files]

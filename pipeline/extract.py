"""
Extract module - Extract data from CSV files (DETRAN vehicle fleet).

This module handles reading CSV files from DETRAN and preparing
the data for loading into staging.
"""

import pandas as pd
from typing import List
from pathlib import Path


# Expected columns in DETRAN CSV (semicolon-separated)
DETRAN_CSV_SEP = ";"
DETRAN_REQUIRED_COLUMNS = [
    "UF",
    "Marca Modelo",
    "Ano Fabricação Veículo CRV",
    "Qtd. Veículos",
]


def read_csv_file(file_path: str, sep: str = DETRAN_CSV_SEP) -> pd.DataFrame:
    """
    Read DETRAN CSV file and return as DataFrame.

    Args:
        file_path: Path to CSV file
        sep: Column separator (default semicolon for DETRAN)

    Returns:
        DataFrame with raw data from CSV

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    file_path_obj = Path(file_path)

    if not file_path_obj.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    if file_path_obj.suffix.lower() not in [".csv"]:
        raise ValueError(f"Invalid file format. Expected .csv, got: {file_path_obj.suffix}")

    try:
        df = pd.read_csv(file_path, sep=sep, header=0, encoding="utf-8")
        return df
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, sep=sep, header=0, encoding="latin-1")
        return df
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {str(e)}")


def validate_csv_structure(df: pd.DataFrame) -> bool:
    """
    Validate that CSV has expected DETRAN columns.

    Args:
        df: DataFrame from CSV

    Returns:
        True if structure is valid

    Raises:
        ValueError: If required columns are missing
    """
    if df.empty:
        raise ValueError("CSV file is empty")

    df_columns_lower = [str(col).strip().lower() for col in df.columns]
    missing_columns = []

    for req_col in DETRAN_REQUIRED_COLUMNS:
        if req_col.strip().lower() not in df_columns_lower:
            missing_columns.append(req_col)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {', '.join(missing_columns)}. "
            f"Found columns: {', '.join(df.columns.tolist())}"
        )

    return True


def list_csv_files(directory: str) -> List[str]:
    """
    List all CSV files in directory.

    Args:
        directory: Directory to search

    Returns:
        List of CSV file paths (absolute paths)
    """
    dir_path = Path(directory)

    if not dir_path.exists():
        return []

    csv_files = list(dir_path.glob("*.csv"))
    return [str(f.absolute()) for f in csv_files]

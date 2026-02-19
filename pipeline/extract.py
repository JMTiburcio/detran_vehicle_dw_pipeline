"""
Extract module - Extract data from CSV files (DETRAN vehicle fleet).

This module handles reading CSV files from DETRAN and preparing
the data for loading into staging. Report period is determined only by
directory name under input (YYYYMM), not by CSV filename.
"""

import re
import pandas as pd
from typing import List, Optional, Tuple
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


def list_period_dirs(input_base: str) -> List[int]:
    """
    List period directories under input_base. A valid period dir has
    a name that is exactly 6 digits (YYYYMM).

    Args:
        input_base: Base directory (e.g. data/input)

    Returns:
        Sorted list of period integers (e.g. [202501, 202506])
    """
    base = Path(input_base)
    if not base.exists() or not base.is_dir():
        return []
    periods = []
    for p in base.iterdir():
        if p.is_dir() and re.match(r"^\d{6}$", p.name):
            try:
                periods.append(int(p.name))
            except ValueError:
                continue
    return sorted(periods)


def resolve_period_and_input_dir(
    input_base: str,
    period_arg: Optional[int],
) -> Tuple[int, Path]:
    """
    Resolve report period and input directory for the pipeline.

    If period_arg is set, use it and validate that the directory exists.
    Otherwise, use the most recent period directory (max YYYYMM). If no
    period directory exists, raise ValueError.

    Args:
        input_base: Base directory (e.g. data/input)
        period_arg: Optional period from CLI (e.g. 202501), or None

    Returns:
        (report_period: int, input_dir: Path)

    Raises:
        ValueError: If no period dirs exist or specified period dir does not exist
    """
    base = Path(input_base)
    if not base.exists():
        raise ValueError(f"Diretório base não existe: {base}")

    if period_arg is not None:
        period_dir = base / str(period_arg)
        if not period_dir.is_dir():
            raise ValueError(
                f"Diretório do período não encontrado: {period_dir}. "
                "Crie o diretório YYYYMM em data/input (ex.: 202501)."
            )
        return period_arg, period_dir

    periods = list_period_dirs(input_base)
    if not periods:
        raise ValueError(
            "Nenhum diretório de período encontrado em data/input. "
            "Crie um diretório com nome YYYYMM (ex.: 202501) e coloque o CSV dentro."
        )
    report_period = max(periods)
    input_dir = base / str(report_period)
    return report_period, input_dir

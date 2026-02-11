"""
Load module - Load raw data into staging layer.

This module handles inserting raw data from DETRAN CSV into
staging.detran_vehicle_raw table.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """
    Create database connection from environment variables.
    
    Returns:
        psycopg2 connection object
        
    Raises:
        ValueError: If required environment variables are missing
    """
    from pipeline.utils import get_db_connection_from_env
    return get_db_connection_from_env()


def create_staging_schemas(conn):
    """
    Create staging schema if it doesn't exist.
    
    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()


def ensure_staging_table_exists(conn=None):
    """
    Ensure staging schema and detran_vehicle_raw table exist.
    Creates them if they don't exist.
    
    Args:
        conn: Database connection (if None, creates new)
        
    Returns:
        tuple: (schema_created: bool, table_created: bool)
    """
    from pipeline.utils import get_db_connection_from_env, execute_sql_file
    from pathlib import Path
    
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False
    
    try:
        cursor = conn.cursor()
        schema_created = False
        table_created = False
        
        # Check if schema exists
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'staging'
        """)
        schema_exists = cursor.fetchone() is not None
        
        if not schema_exists:
            create_staging_schemas(conn)
            schema_created = True
        
        # Check if table exists
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'staging' 
            AND table_name = 'detran_vehicle_raw'
        """)
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            sql_file = Path(__file__).parent.parent / "sql" / "staging" / "02_create_raw_table.sql"
            execute_sql_file(str(sql_file), conn)
            table_created = True
        
        cursor.close()
        return schema_created, table_created
        
    finally:
        if close_conn:
            conn.close()


def load_raw_data(
    df: pd.DataFrame,
    table_name: str = "staging.detran_vehicle_raw",
    source_file: Optional[str] = None,
    conn: Optional[psycopg2.extensions.connection] = None
) -> int:
    """
    Load raw DataFrame into staging table.

    Args:
        df: DataFrame with raw data from DETRAN CSV
        table_name: Target table name
        source_file: Path to source CSV file (for metadata)
        conn: Database connection (if None, creates new)

    Returns:
        Number of rows inserted
    """
    import unicodedata
    import re
    
    # Get or create connection
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    else:
        close_conn = False
    
    try:
        # Normalize column names (same logic as analyze_excel.py)
        def normalize_column_name(col_name: str) -> str:
            normalized = col_name.lower()
            normalized = unicodedata.normalize('NFD', normalized)
            normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
            normalized = re.sub(r'[\s\-]+', '_', normalized)
            normalized = re.sub(r'[^a-z0-9_]', '', normalized)
            normalized = re.sub(r'_+', '_', normalized)
            normalized = normalized.strip('_')
            return normalized if normalized else 'coluna_' + str(hash(col_name) % 10000)
        
        # Create mapping from Excel columns to SQL columns
        column_mapping = {}
        for excel_col in df.columns:
            sql_col = normalize_column_name(excel_col)
            column_mapping[excel_col] = sql_col
        
        # Prepare DataFrame with normalized column names
        df_mapped = df.copy()
        df_mapped.rename(columns=column_mapping, inplace=True)
        
        # Add metadata columns
        source_file_name = Path(source_file).name if source_file else None
        df_mapped['source_file'] = source_file_name
        df_mapped['csv_row'] = range(2, len(df_mapped) + 2)  # CSV rows: 1 = header, data from 2
        
        # Get list of SQL columns (excluding id_raw which is SERIAL)
        sql_columns = [col for col in df_mapped.columns if col not in ['id_raw']]
        
        # Prepare data for insertion
        # Raw table stores everything as text (VARCHAR) to preserve original format
        # Type conversion will be done in normalized table (Phase 2)
        values = []
        for _, row in df_mapped.iterrows():
            row_values = []
            for col in sql_columns:
                val = row[col]
                # Convert to string, preserving original format
                if pd.isna(val):
                    row_values.append(None)
                else:
                    # Store as string (raw data, no conversion)
                    row_values.append(str(val).strip() if str(val).strip() else None)
            values.append(tuple(row_values))
        
        # Build INSERT statement template
        columns_str = ', '.join(sql_columns)
        insert_template = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
        
        # Execute insert using execute_values for batch insertion
        cursor = conn.cursor()
        try:
            execute_values(
                cursor,
                insert_template,
                values,
                page_size=1000  # Insert in batches of 1000
            )
            conn.commit()
            rows_inserted = len(values)  # execute_values doesn't set rowcount, use len of values
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error inserting data: {str(e)}")
        finally:
            cursor.close()
        
        return rows_inserted
        
    finally:
        if close_conn:
            conn.close()


def truncate_staging_table(
    table_name: str = "staging.detran_vehicle_raw",
    conn: Optional[psycopg2.extensions.connection] = None
):
    """
    Truncate staging table (for reprocessing).

    Uses CASCADE so that tables referencing this one (e.g. detran_vehicle_norm
    referencing detran_vehicle_raw) are truncated in the same command.
    
    Args:
        table_name: Table to truncate
        conn: Database connection (if None, creates new)
    """
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    else:
        close_conn = False
    
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error truncating table: {str(e)}")
        finally:
            cursor.close()
    finally:
        if close_conn:
            conn.close()

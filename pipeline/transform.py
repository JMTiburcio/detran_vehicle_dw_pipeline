"""
Transform module - Transform data for core layer.

This module handles:
- Hash generation for unique record identification (hash_veiculo)
- Upsert into core.dim_veiculo (INSERT if hash doesn't exist, UPDATE if exists)
- Trigger on dim_veiculo automatically writes history to core.audit_dim_veiculo
"""

import hashlib
import pandas as pd
import psycopg2
from pathlib import Path
from typing import Optional, Dict, Tuple

from pipeline.utils import get_db_connection_from_env, execute_sql_file


def generate_hash_veiculo(
    brand_norm: Optional[str],
    model_norm: Optional[str],
    version_norm: Optional[str],
    fuel_norm: Optional[str],
    transmission_norm: Optional[str],
    aspiration_norm: Optional[str],
    engine_config_norm: Optional[str],
    engine_name_norm: Optional[str],
    codigo_fraga: Optional[str],
    year: Optional[int],
) -> str:
    """
    Generate hash_veiculo from normalized attributes.

    Hash components (in order):
    - brand_norm, model_norm, version_norm, fuel_norm, transmission_norm,
    - aspiration_norm, engine_config_norm, engine_name_norm, codigo_fraga, year

    Returns:
        SHA256 hash string (64 characters)
    """
    components = [
        str(brand_norm) if brand_norm is not None else "",
        str(model_norm) if model_norm is not None else "",
        str(version_norm) if version_norm is not None else "",
        str(fuel_norm) if fuel_norm is not None else "",
        str(transmission_norm) if transmission_norm is not None else "",
        str(aspiration_norm) if aspiration_norm is not None else "",
        str(engine_config_norm) if engine_config_norm is not None else "",
        str(engine_name_norm) if engine_name_norm is not None else "",
        str(codigo_fraga) if codigo_fraga is not None else "",
        str(year) if year is not None else "",
    ]
    hash_string = "|".join(components)
    hash_bytes = hashlib.sha256(hash_string.encode("utf-8")).digest()
    return hash_bytes.hex()


# Columns to load from norm into core (same as core.dim_veiculo minus id_veiculo, created_at, updated_at)
CORE_DIM_COLUMNS = [
    "hash_veiculo",
    "fraga_code",
    "brand",
    "model",
    "version",
    "engine_name",
    "engine_config",
    "power_cv",
    "fuel",
    "aspiration",
    "ktype_tecdoc",
    "classification",
    "vehicle_type",
    "vehicle_category",
    "generation",
    "displacement_liters",
    "command",
    "cylinder_count",
    "transmission",
    "cabin_type",
    "fipe_code",
    "brand_norm",
    "model_norm",
    "version_norm",
    "engine_name_norm",
    "engine_config_norm",
    "fuel_norm",
    "transmission_norm",
    "aspiration_norm",
    "year",
    "load_timestamp",
    "source_file",
    "excel_row",
]


def ensure_core_tables_exist(conn: Optional[psycopg2.extensions.connection] = None) -> Tuple[bool, bool]:
    """
    Ensure core schema and tables (dim_veiculo, audit_dim_veiculo) and trigger exist.

    Runs sql/core/01, 02, 03, 04 in order.

    Returns:
        (schema_created: bool, tables_created: bool)
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    base = Path(__file__).parent.parent / "sql" / "core"
    schema_created = False
    tables_created = False

    try:
        cursor = conn.cursor()

        # 01 - Schema
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'core'")
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "01_create_schema.sql"), conn)
            schema_created = True

        # 02 - dim_veiculo
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = 'dim_veiculo'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "02_create_dim_veiculo.sql"), conn)
            tables_created = True

        # 03 - audit_dim_veiculo
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = 'audit_dim_veiculo'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "03_create_audit_table.sql"), conn)
            tables_created = True

        # 04 - Trigger (recreate to keep in sync with table structure)
        execute_sql_file(str(base / "04_create_trigger.sql"), conn)

        cursor.close()
        return schema_created, tables_created
    finally:
        if close_conn:
            conn.close()


def _row_to_core_values(row: pd.Series) -> tuple:
    """Build a tuple of values for core.dim_veiculo in CORE_DIM_COLUMNS order (hash already in row)."""
    out = []
    for col in CORE_DIM_COLUMNS:
        val = row.get(col)
        if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
            out.append(None)
        else:
            out.append(val)
    return tuple(out)


def upsert_dim_veiculo(
    df: pd.DataFrame,
    table_name: str = "core.dim_veiculo",
    conn: Optional[psycopg2.extensions.connection] = None,
) -> Dict[str, int]:
    """
    Upsert normalized data into core.dim_veiculo.

    - If hash_veiculo does not exist -> INSERT (trigger logs to audit).
    - If hash_veiculo exists -> UPDATE (trigger logs changes to audit).

    Args:
        df: DataFrame with norm columns + hash_veiculo (from add_hash_to_norm_df or similar).
        table_name: Target table.
        conn: Database connection.

    Returns:
        {'inserted': int, 'updated': int}
    """
    from psycopg2.extras import execute_values

    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    inserted = 0
    updated = 0

    try:
        if len(df) == 0:
            return {"inserted": 0, "updated": 0}

        cols_no_hash = [c for c in CORE_DIM_COLUMNS if c != "hash_veiculo"]
        columns_str = ", ".join(CORE_DIM_COLUMNS)
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols_no_hash)

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES %s
            ON CONFLICT (hash_veiculo) DO UPDATE SET {update_set}
        """

        values = [_row_to_core_values(row) for _, row in df.iterrows()]

        cursor = conn.cursor()
        try:
            execute_values(cursor, sql, values, page_size=1000)
            conn.commit()
            # We don't have per-row insert/update count from execute_values; report total as inserted for simplicity
            inserted = len(values)
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error upserting into core.dim_veiculo: {str(e)}") from e
        finally:
            cursor.close()

        return {"inserted": inserted, "updated": updated}
    finally:
        if close_conn:
            conn.close()


def add_hash_to_norm_df(df_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Add column hash_veiculo to a DataFrame that has norm columns (from staging.fraga_vehicle_norm).

    Drops id_norm, id_raw, data_carga if present so the result is ready for core.dim_veiculo.
    """
    df = df_norm.copy()
    for drop in ("id_norm", "id_raw", "data_carga"):
        if drop in df.columns:
            df = df.drop(columns=[drop])

    hashes = []
    for _, row in df.iterrows():
        h = generate_hash_veiculo(
            brand_norm=row.get("brand_norm"),
            model_norm=row.get("model_norm"),
            version_norm=row.get("version_norm"),
            fuel_norm=row.get("fuel_norm"),
            transmission_norm=row.get("transmission_norm"),
            aspiration_norm=row.get("aspiration_norm"),
            engine_config_norm=row.get("engine_config_norm"),
            engine_name_norm=row.get("engine_name_norm"),
            codigo_fraga=row.get("fraga_code"),
            year=row.get("year"),
        )
        hashes.append(h)
    df["hash_veiculo"] = hashes
    return df

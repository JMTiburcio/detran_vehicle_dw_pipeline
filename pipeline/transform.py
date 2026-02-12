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


# ============================================================================
# DETRAN-specific functions (dimensional model: dim_veiculo + fato_frota_uf)
# ============================================================================

def generate_hash_veiculo_detran(
    marca: Optional[str],
    modelo: Optional[str],
    ano_fabricacao: Optional[int],
) -> str:
    """
    Generate hash_veiculo for DETRAN dim_veiculo from marca + modelo + ano_fabricacao.

    Returns:
        SHA256 hash string (64 characters)
    """
    components = [
        str(marca) if marca is not None else "",
        str(modelo) if modelo is not None else "",
        str(ano_fabricacao) if ano_fabricacao is not None else "",
    ]
    hash_string = "|".join(components)
    hash_bytes = hashlib.sha256(hash_string.encode("utf-8")).digest()
    return hash_bytes.hex()


DIM_VEICULO_COLUMNS = ["hash_veiculo", "marca", "modelo", "ano_fabricacao", "descricao_detran"]
FATO_FROTA_COLUMNS = ["id_veiculo", "uf", "frota", "id_raw"]


def prepare_dim_veiculo_from_norm(df_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare dim_veiculo DataFrame from norm: unique vehicles (marca, modelo, ano_fabricacao, descricao_detran).

    Returns:
        DataFrame with columns: hash_veiculo, marca, modelo, ano_fabricacao, descricao_detran
    """
    df = df_norm.copy()
    
    # Generate hash for each row
    hashes = []
    for _, row in df.iterrows():
        h = generate_hash_veiculo_detran(
            marca=row.get("marca"),
            modelo=row.get("modelo"),
            ano_fabricacao=row.get("ano_fabricacao"),
        )
        hashes.append(h)
    df["hash_veiculo"] = hashes
    
    # Keep unique vehicles (marca, modelo, ano, descricao) - drop duplicates by hash_veiculo
    # Keep first occurrence of each hash (arbitrary choice for descricao_detran if different)
    df_dim = df[["hash_veiculo", "marca", "modelo", "ano_fabricacao", "descricao_detran"]].drop_duplicates(subset=["hash_veiculo"])
    
    return df_dim


def ensure_core_detran_tables_exist(conn: Optional[psycopg2.extensions.connection] = None) -> Tuple[bool, bool]:
    """
    Ensure core schema and DETRAN tables (dim_veiculo_detran, fato_frota_uf, audits, triggers).

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

        # 02 - dim_veiculo_detran
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = 'dim_veiculo_detran'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "02_create_dim_veiculo_detran.sql"), conn)
            tables_created = True

        # 03 - fato_frota_uf
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = 'fato_frota_uf'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "03_create_fato_frota_uf.sql"), conn)
            tables_created = True

        # 04 - audit_dim_veiculo_detran
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = 'audit_dim_veiculo_detran'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "04_create_audit_dim_veiculo.sql"), conn)
            tables_created = True

        # 05 - audit_fato_frota_uf
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = 'audit_fato_frota_uf'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "05_create_audit_fato_frota.sql"), conn)
            tables_created = True

        # 06 & 07 - Triggers (recreate to keep in sync)
        execute_sql_file(str(base / "06_create_trigger_dim_veiculo.sql"), conn)
        execute_sql_file(str(base / "07_create_trigger_fato_frota.sql"), conn)

        cursor.close()
        return schema_created, tables_created
    finally:
        if close_conn:
            conn.close()


def _row_to_dim_veiculo_values(row: pd.Series) -> tuple:
    """Build tuple for dim_veiculo_detran."""
    return tuple(row.get(col) if not pd.isna(row.get(col)) else None for col in DIM_VEICULO_COLUMNS)


def upsert_dim_veiculo_detran(
    df_dim: pd.DataFrame,
    table_name: str = "core.dim_veiculo_detran",
    conn: Optional[psycopg2.extensions.connection] = None,
) -> int:
    """
    Upsert dim_veiculo_detran.

    Args:
        df_dim: DataFrame with columns: hash_veiculo, marca, modelo, ano_fabricacao, descricao_detran
        table_name: Target table
        conn: Database connection

    Returns:
        Number of rows upserted
    """
    from psycopg2.extras import execute_values

    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        if len(df_dim) == 0:
            return 0

        cols_no_hash = [c for c in DIM_VEICULO_COLUMNS if c != "hash_veiculo"]
        columns_str = ", ".join(DIM_VEICULO_COLUMNS)
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols_no_hash)

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES %s
            ON CONFLICT (hash_veiculo) DO UPDATE SET {update_set}
        """

        values = [_row_to_dim_veiculo_values(row) for _, row in df_dim.iterrows()]

        cursor = conn.cursor()
        try:
            execute_values(cursor, sql, values, page_size=1000)
            conn.commit()
            return len(values)
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error upserting dim_veiculo_detran: {str(e)}") from e
        finally:
            cursor.close()
    finally:
        if close_conn:
            conn.close()


def get_id_veiculo_from_hashes(
    hashes: list,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> pd.DataFrame:
    """
    Query dim_veiculo_detran to get id_veiculo for given hash_veiculo list.

    Returns:
        DataFrame with columns: hash_veiculo, id_veiculo
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        if not hashes:
            return pd.DataFrame(columns=["hash_veiculo", "id_veiculo"])

        # Use parameterized query with IN clause
        placeholders = ",".join(["%s"] * len(hashes))
        query = f"SELECT hash_veiculo, id_veiculo FROM core.dim_veiculo_detran WHERE hash_veiculo IN ({placeholders})"
        df = pd.read_sql(query, conn, params=hashes)
        return df
    finally:
        if close_conn:
            conn.close()


def _row_to_fato_frota_values(row: pd.Series) -> tuple:
    """Build tuple for fato_frota_uf."""
    return tuple(row.get(col) if not pd.isna(row.get(col)) else None for col in FATO_FROTA_COLUMNS)


def upsert_fato_frota_uf(
    df_fato: pd.DataFrame,
    table_name: str = "core.fato_frota_uf",
    conn: Optional[psycopg2.extensions.connection] = None,
) -> int:
    """
    Upsert fato_frota_uf.

    Args:
        df_fato: DataFrame with columns: id_veiculo, uf, frota, id_raw
        table_name: Target table
        conn: Database connection

    Returns:
        Number of rows upserted
    """
    from psycopg2.extras import execute_values

    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        if len(df_fato) == 0:
            return 0

        columns_str = ", ".join(FATO_FROTA_COLUMNS)
        # ON CONFLICT on UNIQUE(id_veiculo, uf) -> UPDATE frota and id_raw
        update_set = "frota = EXCLUDED.frota, id_raw = EXCLUDED.id_raw"

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES %s
            ON CONFLICT (id_veiculo, uf) DO UPDATE SET {update_set}
        """

        values = [_row_to_fato_frota_values(row) for _, row in df_fato.iterrows()]

        cursor = conn.cursor()
        try:
            execute_values(cursor, sql, values, page_size=1000)
            conn.commit()
            return len(values)
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error upserting fato_frota_uf: {str(e)}") from e
        finally:
            cursor.close()
    finally:
        if close_conn:
            conn.close()

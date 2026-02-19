"""
Analytics module - Stable replicas of core tables for BI.

Creates and refreshes analytics.dim_veiculo_detran and analytics.fato_frota_uf
by atomic swap: build into _new tables, then rename current -> _prev, _new -> final.
BI consumes the final tables directly (no VIEWs). Only current and previous version kept.
"""

from pathlib import Path
from typing import Optional, Tuple

import psycopg2

from pipeline.utils import get_db_connection_from_env, execute_sql_file


def ensure_analytics_schema_and_tables(
    conn: Optional[psycopg2.extensions.connection] = None,
) -> Tuple[bool, bool]:
    """
    Ensure analytics schema and _new tables exist (for swap refresh).
    Creates schema and dim_veiculo_detran_new, fato_frota_uf_new if they don't exist.

    Returns:
        (schema_created: bool, tables_created: bool)
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    base = Path(__file__).parent.parent / "sql" / "analytics"
    schema_created = False
    tables_created = False

    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'analytics'"
        )
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "01_create_schema.sql"), conn)
            schema_created = True

        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'analytics' AND table_name = 'dim_veiculo_detran_new'
        """)
        if cursor.fetchone() is None:
            execute_sql_file(str(base / "04_create_new_tables.sql"), conn)
            tables_created = True

        cursor.close()
        return schema_created, tables_created
    finally:
        if close_conn:
            conn.close()


def refresh_analytics_from_core(
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """
    Refresh analytics tables from core by atomic swap.
    1. Ensure schema and _new tables exist.
    2. Truncate _new tables.
    3. Copy core.dim_veiculo_detran -> analytics.dim_veiculo_detran_new.
    4. Copy core.fato_frota_uf -> analytics.fato_frota_uf_new (all partitions).
    5. Swap: if current table exists, DROP _prev, RENAME current -> _prev, RENAME _new -> final.
    Keeps only current and previous version (*_prev).
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        ensure_analytics_schema_and_tables(conn)

        cursor = conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE analytics.dim_veiculo_detran_new;")
            cursor.execute("TRUNCATE TABLE analytics.fato_frota_uf_new;")
            conn.commit()

            cursor.execute(
                "INSERT INTO analytics.dim_veiculo_detran_new "
                "SELECT id_veiculo, hash_veiculo, marca, modelo, ano_fabricacao, descricao_detran, created_at, updated_at "
                "FROM core.dim_veiculo_detran"
            )
            cursor.execute(
                "INSERT INTO analytics.fato_frota_uf_new "
                "SELECT report_period, id_fato, id_veiculo, uf, frota, id_raw, created_at, updated_at "
                "FROM core.fato_frota_uf"
            )
            conn.commit()

            # Swap: current -> _prev, _new -> current (only current and previous kept)
            for base_name in ("dim_veiculo_detran", "fato_frota_uf"):
                cursor.execute("""
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'analytics' AND table_name = %s
                """, (base_name,))
                if cursor.fetchone() is not None:
                    cursor.execute(f"DROP TABLE IF EXISTS analytics.{base_name}_prev;")
                    cursor.execute(f"ALTER TABLE analytics.{base_name} RENAME TO {base_name}_prev;")
                cursor.execute(f"ALTER TABLE analytics.{base_name}_new RENAME TO {base_name};")
            conn.commit()
            # _new tables are gone (renamed to final); only current and _prev remain.
            # Next run will recreate _new in ensure_analytics_schema_and_tables.
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error refreshing analytics from core: {str(e)}") from e
        finally:
            cursor.close()
    finally:
        if close_conn:
            conn.close()

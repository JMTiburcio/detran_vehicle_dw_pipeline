"""
Normalize module - Normalize and standardize vehicle data.

This module handles:
- Text normalization (uppercase, accents, spaces, special characters)
- Domain standardization (brands, fuels, transmissions) using dictionaries
- Year explosion (one row per year in date range)
- Load to staging.fraga_vehicle_norm via INSERT only (table is truncated before load).
"""

import pandas as pd
import unicodedata
import re
from typing import Dict, Optional, List
from pipeline.utils import get_db_connection_from_env, execute_sql_file
from pathlib import Path
from psycopg2.extras import execute_values


# TODO: Fase 2 - Popular dicionários de normalização baseado em análise dos dados
NORMALIZE_BRANDS: Dict[str, str] = {
    # Exemplo: "VW" -> "VOLKSWAGEN", "ford" -> "FORD"
    # TODO: Adicionar mapeamentos reais após análise dos dados
}

NORMALIZE_FUELS: Dict[str, str] = {
    # Exemplo: "GASOLINA" -> "GASOLINA", "Flex" -> "FLEX", etc.
    # TODO: Adicionar mapeamentos reais após análise dos dados
}

NORMALIZE_TRANSMISSIONS: Dict[str, str] = {
    # Exemplo: "MANUAL" -> "MANUAL", "AUTOMATICA" -> "AUTOMATICA", etc.
    # TODO: Adicionar mapeamentos reais após análise dos dados
}

NORMALIZE_ASPIRATIONS: Dict[str, str] = {
    # Exemplo: "ASPIRADO" -> "ASPIRADO", "TURBO" -> "TURBO", etc.
    # TODO: Adicionar mapeamentos reais após análise dos dados
}


def normalize_text(text: Optional[str]) -> Optional[str]:
    """
    Normalize text: uppercase, remove accents, remove special characters (except +),
    normalize spaces.
    
    Rules:
    - Uppercase
    - Remove accents (á -> a, é -> e, ç -> c, etc.)
    - Remove special characters except "+" (to differentiate Ford KA from Ford KA+)
    - Normalize spaces (trim, remove multiple spaces)
    - Treat "-" and empty strings as None
    
    Args:
        text: Input text (can be None)
        
    Returns:
        Normalized text or None
    """
    if text is None:
        return None
    
    # Convert to string and strip
    text = str(text).strip()
    
    # Treat "-" and empty strings as None
    if text == "-" or text == "":
        return None
    
    # Convert to uppercase
    text = text.upper()
    
    # Remove accents (NFD normalization and remove combining marks)
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        
    # Normalize spaces: trim and replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    # Return None if empty after normalization
    if text == "":
        return None
    
    return text


def normalize_brand(brand: Optional[str]) -> Optional[str]:
    """
    Normalize brand using dictionary.
    
    Args:
        brand: Raw brand value
        
    Returns:
        Normalized brand
    """
    # First normalize text
    normalized = normalize_text(brand)
    if normalized is None:
        return None
    
    # TODO: Lookup in NORMALIZE_BRANDS dictionary
    # For now, return normalized text
    # if normalized in NORMALIZE_BRANDS:
    #     return NORMALIZE_BRANDS[normalized]
    
    return normalized


def normalize_fuel(fuel: Optional[str]) -> Optional[str]:
    """
    Normalize fuel using dictionary.
    
    Args:
        fuel: Raw fuel value
        
    Returns:
        Normalized fuel
    """
    # First normalize text
    normalized = normalize_text(fuel)
    if normalized is None:
        return None
    
    # TODO: Lookup in NORMALIZE_FUELS dictionary
    # For now, return normalized text
    # if normalized in NORMALIZE_FUELS:
    #     return NORMALIZE_FUELS[normalized]
    
    return normalized


def normalize_transmission(transmission: Optional[str]) -> Optional[str]:
    """
    Normalize transmission using dictionary.
    
    Args:
        transmission: Raw transmission value
        
    Returns:
        Normalized transmission
    """
    # First normalize text
    normalized = normalize_text(transmission)
    if normalized is None:
        return None
    
    # TODO: Lookup in NORMALIZE_TRANSMISSIONS dictionary
    # For now, return normalized text
    # if normalized in NORMALIZE_TRANSMISSIONS:
    #     return NORMALIZE_TRANSMISSIONS[normalized]
    
    return normalized


def normalize_aspiration(aspiration: Optional[str]) -> Optional[str]:
    """
    Normalize aspiration using dictionary.
    
    Args:
        aspiration: Raw aspiration value
        
    Returns:
        Normalized aspiration
    """
    # First normalize text
    normalized = normalize_text(aspiration)
    if normalized is None:
        return None
    
    # TODO: Lookup in NORMALIZE_ASPIRATIONS dictionary
    # For now, return normalized text
    # if normalized in NORMALIZE_ASPIRATIONS:
    #     return NORMALIZE_ASPIRATIONS[normalized]
    
    return normalized


def explode_by_year(
    data_inicio: Optional[str],
    data_final: Optional[str]
) -> List[int]:
    """
    Explode date range into list of years.
    
    If data_final is None or empty, assume it equals data_inicio (single year).
    
    Args:
        data_inicio: Start year (as string from raw table)
        data_final: End year (as string from raw table, can be None)
        
    Returns:
        List of years (integers)
    """
    # Parse start year
    try:
        start_year = int(float(str(data_inicio))) if data_inicio else None
    except (ValueError, TypeError):
        start_year = None
    
    if start_year is None:
        return []
    
    # Parse end year
    if data_final is None or str(data_final).strip() == "" or str(data_final).strip() == "-":
        # If no end date, assume same as start (single year)
        end_year = start_year
    else:
        try:
            end_year = int(float(str(data_final)))
        except (ValueError, TypeError):
            # If invalid, assume same as start
            end_year = start_year
    
    # Generate list of years (inclusive range)
    years = list(range(start_year, end_year + 1))
    
    return years


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize entire DataFrame from raw table.
    
    This function:
    1. Normalizes text fields (brand, model, version, engine, fuel, transmission, aspiration)
    2. Explodes records by year
    
    Result is loaded into staging.fraga_vehicle_norm via INSERT only (no hash, no upsert).
    The norm table is truncated before each load.
    
    Args:
        df: Raw DataFrame from staging.fraga_vehicle_raw
        
    Returns:
        Normalized DataFrame ready for staging.fraga_vehicle_norm
    """
    # Create list to store exploded rows
    normalized_rows = []
    
    # Helper function to treat "-" and empty strings as None
    def clean_value(val):
        if val is None or pd.isna(val):
            return None
        val_str = str(val).strip()
        if val_str == "-" or val_str == "":
            return None
        return val_str
    
    # Process each row
    for _, row in df.iterrows():
        # Get all original values from raw table
        codigo_fraga = row.get('codigo_fraga')
        marca = row.get('marca')
        modelo_veiculo = row.get('modelo_veiculo')
        nome_veiculo = row.get('nome_veiculo')
        nome_motor = row.get('nome_motor')
        configuracao_motor = row.get('configuracao_motor')
        potencia_cv = row.get('potencia_cv')
        combustivel = row.get('combustivel')
        aspiracao = row.get('aspiracao')
        ktype_tecdoc = row.get('ktype_tecdoc')
        classificacao = row.get('classificacao')
        tipo_veiculo = row.get('tipo_veiculo')
        categoria_veiculo = row.get('categoria_veiculo')
        geracao = row.get('geracao')
        cilindrada_litros = row.get('cilindrada_litros')
        comando = row.get('comando')
        numero_cilindros = row.get('numero_cilindros')
        tipo_transmissao = row.get('tipo_transmissao')
        data_inicio = row.get('data_inicio')
        data_final = row.get('data_final')
        tipo_cabine = row.get('tipo_cabine')
        codigo_fipe = row.get('codigo_fipe')
        load_timestamp = row.get('load_timestamp')
        source_file = row.get('source_file')
        excel_row = row.get('excel_row')
        id_raw = row.get('id_raw')
        
        # Normalize text fields (for _norm columns)
        brand_norm = normalize_brand(marca)
        model_norm = normalize_text(modelo_veiculo)
        version_norm = normalize_text(nome_veiculo)
        engine_name_norm = normalize_text(nome_motor)
        engine_config_norm = normalize_text(configuracao_motor)
        fuel_norm = normalize_fuel(combustivel)
        transmission_norm = normalize_transmission(tipo_transmissao)
        aspiration_norm = normalize_aspiration(aspiracao)
        
        # Explode by year
        years = explode_by_year(data_inicio, data_final)
        
        # If no valid years, skip this row
        if not years:
            continue
        
        # Create one row per year
        for year in years:            
            # Create normalized row with all columns translated to English
            normalized_row = {
                # Original columns (translated to English)
                'fraga_code': clean_value(codigo_fraga),
                'brand': clean_value(marca),
                'model': clean_value(modelo_veiculo),
                'version': clean_value(nome_veiculo),
                'engine_name': clean_value(nome_motor),
                'engine_config': clean_value(configuracao_motor),
                'power_cv': clean_value(potencia_cv),
                'fuel': clean_value(combustivel),
                'aspiration': clean_value(aspiracao),
                'ktype_tecdoc': clean_value(ktype_tecdoc),
                'classification': clean_value(classificacao),
                'vehicle_type': clean_value(tipo_veiculo),
                'vehicle_category': clean_value(categoria_veiculo),
                'generation': clean_value(geracao),
                'displacement_liters': clean_value(cilindrada_litros),
                'command': clean_value(comando),
                'cylinder_count': clean_value(numero_cilindros),
                'transmission': clean_value(tipo_transmissao),
                'cabin_type': clean_value(tipo_cabine),
                'fipe_code': clean_value(codigo_fipe),
                # Normalized columns (English - normalized text)
                'brand_norm': brand_norm,
                'model_norm': model_norm,
                'version_norm': version_norm,
                'engine_name_norm': engine_name_norm,
                'engine_config_norm': engine_config_norm,
                'fuel_norm': fuel_norm,
                'transmission_norm': transmission_norm,
                'aspiration_norm': aspiration_norm,
                # Year
                'year': year,
                # Metadata columns
                'load_timestamp': load_timestamp,
                'source_file': source_file,
                'excel_row': excel_row,
                'id_raw': id_raw
            }
            
            normalized_rows.append(normalized_row)
    
    # Create DataFrame from normalized rows
    if normalized_rows:
        df_norm = pd.DataFrame(normalized_rows)
    else:
        # Return empty DataFrame with correct columns (all translated to English)
        df_norm = pd.DataFrame(columns=[
            # Original columns (English)
            'fraga_code', 'brand', 'model', 'version', 'engine_name', 'engine_config',
            'power_cv', 'fuel', 'aspiration', 'ktype_tecdoc', 'classification',
            'vehicle_type', 'vehicle_category', 'generation', 'displacement_liters',
            'command', 'cylinder_count', 'transmission', 'cabin_type', 'fipe_code',
            # Normalized columns (English)
            'brand_norm', 'model_norm', 'version_norm', 'engine_name_norm',
            'engine_config_norm', 'fuel_norm', 'transmission_norm', 'aspiration_norm',
            # Year and metadata
            'year', 'load_timestamp', 'source_file', 'excel_row', 'id_raw'
        ])
    
    return df_norm


def ensure_norm_table_exists(conn=None):
    """
    Ensure staging schema and fraga_vehicle_norm table exist.
    Creates them if they don't exist.
    
    Args:
        conn: Database connection (if None, creates new)
        
    Returns:
        tuple: (schema_created: bool, table_created: bool)
    """
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
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            conn.commit()
            schema_created = True
        
        # Check if table exists
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'staging' 
            AND table_name = 'fraga_vehicle_norm'
        """)
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            sql_file = Path(__file__).parent.parent / "sql" / "staging" / "03_create_norm_table.sql"
            execute_sql_file(str(sql_file), conn)
            table_created = True
        
        cursor.close()
        return schema_created, table_created
        
    finally:
        if close_conn:
            conn.close()


def read_raw_data(
    conn: Optional = None
) -> pd.DataFrame:
    """
    Read raw data from staging.fraga_vehicle_raw.
    
    Args:
        conn: Database connection (if None, creates new)
        
    Returns:
        DataFrame with raw data
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False
    
    try:
        query = "SELECT * FROM staging.fraga_vehicle_raw"
        df = pd.read_sql(query, conn)
        return df
    finally:
        if close_conn:
            conn.close()


def read_norm_data(
    conn: Optional = None
) -> pd.DataFrame:
    """
    Read normalized data from staging.fraga_vehicle_norm.
    
    Args:
        conn: Database connection (if None, creates new)
        
    Returns:
        DataFrame with normalized data (same columns as norm table, excluding id_norm)
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False
    
    try:
        query = "SELECT * FROM staging.fraga_vehicle_norm"
        df = pd.read_sql(query, conn)
        return df
    finally:
        if close_conn:
            conn.close()


def load_normalized_to_staging(
    df: pd.DataFrame,
    table_name: str = "staging.fraga_vehicle_norm",
    conn: Optional = None
) -> int:
    """
    Load normalized DataFrame into staging.fraga_vehicle_norm.
    
    Uses INSERT only (no upsert). Caller must truncate the table before load
    if reprocessing is desired.
    
    Args:
        df: Normalized DataFrame
        table_name: Target table name
        conn: Database connection (if None, creates new)
        
    Returns:
        Number of rows inserted
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False
    
    try:
        if len(df) == 0:
            return 0
        
        # Prepare data for insertion (all columns translated to English)
        columns = [
            # Original columns (English)
            'fraga_code', 'brand', 'model', 'version', 'engine_name', 'engine_config',
            'power_cv', 'fuel', 'aspiration', 'ktype_tecdoc', 'classification',
            'vehicle_type', 'vehicle_category', 'generation', 'displacement_liters',
            'command', 'cylinder_count', 'transmission', 'cabin_type', 'fipe_code',
            # Normalized columns (English)
            'brand_norm', 'model_norm', 'version_norm', 'engine_name_norm',
            'engine_config_norm', 'fuel_norm', 'transmission_norm', 'aspiration_norm',
            # Year and metadata
            'year', 'load_timestamp', 'source_file', 'excel_row', 'id_raw'
        ]
        
        values = []
        for _, row in df.iterrows():
            row_values = []
            for col in columns:
                val = row.get(col)
                # Convert to None if NaN or empty string
                if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
                    row_values.append(None)
                else:
                    row_values.append(val)
            values.append(tuple(row_values))
        
        # Build INSERT statement
        columns_str = ', '.join(columns)
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
            rows_inserted = len(values)
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error inserting normalized data: {str(e)}")
        finally:
            cursor.close()
        
        return rows_inserted
        
    finally:
        if close_conn:
            conn.close()

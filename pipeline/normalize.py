"""
Normalize module - Normalize DETRAN vehicle raw data.

- Reads from staging.detran_vehicle_raw (same structure as CSV)
- Applies: numeric conversion, marca/modelo split (by "/"), filters by valid brands (sum > 10),
  excludes artesanal (A.) and importado (I/) for brand validation
- Loads to staging.detran_vehicle_norm
"""

import re
import numpy as np
import pandas as pd
from typing import Optional, Tuple
from pipeline.utils import get_db_connection_from_env, execute_sql_file
from pathlib import Path
from psycopg2.extras import execute_values


# Column names in raw table (from DB)
RAW_UF = "uf"
RAW_MARCA_MODELO = "marca_modelo"
RAW_ANO = "ano_fabricacao_veiculo_crv"
RAW_QTD = "qtd_veiculos"
RAW_ID = "id_raw"

# Aliases matching the script/CSV names for the transformation steps
COL_UF = "UF"
COL_MARCA_MODELO = "Marca Modelo"
COL_ANO = "Ano Fabricação Veículo CRV"
COL_QTD = "Qtd. Veículos"

BRANDS = sorted(["ALFA ROMEO","JINBEI","LOTUS","D2D","INTERNATIONAL","MARCOPOLO","CHALLENGER","BIMOTA","PIAGGIO","XCMG","MAN","TVS","HAOBAO","CHAMONIX","DAYI","MALAGUTI","DAF","AUDI","YUKI","PORSCHE","KAHENA","BR JINMA","TINBOT","MATRA","HARVESTER","VOLTZ","HYUNDAI","LAMBORGHINI","CF MOTOS","LUQI","SCANIA","GMC","TROLLER","VOLKSWAGEN","GOWEI","SANTA MATILDE","AMAZONAS","MOTOCAR","JOHNNY PAG","SSANGYONG","MOTTU","FIAT ALLIS","FERRARI","CYKLOS","YAMAHA","INDIAN","BUDNY","LEAPMOTOR","GAC GROUP","SEAT","BRANDY","PUMA","SUPER SOCO","MG MOTORS","MANGOSTEEN","AITO","MITSUBISHI","HUSABERG","GEELY","NETA","MAJ","GENESIS","DFM","WUXI-GWS","WATTS","LAND ROVER","MOTRIZ","HUSQVARNA","TALARIA","ORIGEM","UNIVERSAL","HAFEI","ZEEKR","FNM","DAIHATSU","MV AGUSTA","REGAL RAPTOR","JOHN DEERE","KTM","PIONEIRA","FORD","CHRYSLER","HORWIN","CADILLAC","RAM","LEXUS","ENGESA","DENZA","FORA DA FROTA BRASILEIRA","FUTENGDA","ISUZU","ENVEMO","DAELIM","TAILING","NIU","MILETO","RENAULT","EFFA MOTORS","KENWORTH","JIAPENG VOLCANO","SMART","VMOTO","ANAIG","VENTO","BASHI MOTORS","JETOUR","DAYANG","SILENCE","CHANGAN","CASE IH","SANNYA MOTOR","NAO VEICULAR","BEE","MS ELETRIC","SUNDOWN","JAECOO","SUDU","SINO-GOLD","SMARDA","FIBRAVAN","DAMAC","JAGUAR","WAZN","MGW","BRILSTAR","VOLARE","IVECO","FOTON","MORMAII E-MOTORS","BUELL","DUCATI","KIA","BENTLEY","MASSEY FERGUSON","YADEA","MAZDA","HONDA","MAHINDRA","MONTESA 250","BUGRE","LAVRALE","EGO","CAGIVA","LIFAN","MOTO GUZZI","FENDT","DODGE","MVK","FLYELETRICS","XINGYUE","ROLLS-ROYCE","E-MART CAR","BORAM","PONTIAC","KUHN MONTANA","ROTOM","RIVIAN","WEHAWK","FUSCO MOTOSEGURA","GAS GAS","CALOI","BRM","OMODA","IMPLANOR","CAN-AM","LINCOLN","VESPA","CITROEN","MOTORINO","VALTRA","AGRALE","GLOOV","BRAVIA","MAFERSA","ASIA MOTORS","AIMA","WUYANG","HIGER","SUZUKI","JINYI","DAEWOO","AVELLOZ","BULL","NEW HOLLAND","BRILLIANCE","BAJAJ","YANMAR","ARROW MOBILITY S.A","CHANA","IDEAL","DAFRA","ROYAL ENFIELD","MIURA","ZONTES","GURGEL","WALK","CASE","BABY","TESLA","SWM","RIDDARA","HUMMER","STARA","MRX","TRAXX","MASERATI","ANKAI","MOTO MORINI","KYMCO","PETERBILT","AIPAO","SHINERAY","SYM","HYOSUNG","FYM","JAC","NISSAN","SANYANG","MIZA","MINI","PLA PULVERIZADORES","SUBARU","WAKE","ADLY","LEVA MOTORS","IROS","GWM","TOYOTA","LS TRACTOR","JPX","GREEN","HENREY","GREAT WALL MOTOR","CHERY","AUGURI","FANTIC","JACTO","JEEP","MOTO CHEFE","CHEVROLET","SHENGQI","SPARTAN","SERES","RELY","LUYUAN","PEUGEOT","CROSS LANDER","YAMASAKI","LANDINI","JAN","HARLEY-DAVIDSON","BYD","FYBER","DONGFENG","MCLAREN","DEUTZ","VENTANE MOTORS","SOUSA","SINOTRUK","VAMMO MOTOS","TRIUMPH","KAWASAKI","ARIIC","APRILIA","CBT","KASINSKI","AMAZON","GARINNI","WANGPAI","TAC","JIAYUAN","ASTON MARTIN","LADA","LVNENG E-BIKE","BSA","FEVER","SWM MOTORS","BENELLI","SHACMAN","LETIN","BUGATTI","GCX","HAOJIAN","ACURA","IANOR","FIAT","JONNY","BMW","VOLVO","LAFER","HAOJUE","MERCEDES BENZ"], key=len, reverse=True)

# Dicionário de normalização de marcas
BRAND_NORMALIZATION = {
    "R": "REBOQUE",
    "REB": "REBOQUE",
    "SR": "SEMI REBOQUE",
    "VW": "VOLKSWAGEN",
    "VOLKS": "VOLKSWAGEN",
    "GM": "CHEVROLET",
    "CHEV": "CHEVROLET",
    "MERCEDES-BENZ": "MERCEDES BENZ",
    "MBENZ": "MERCEDES BENZ",
    "HARLEY DAVIDSON": "HARLEY-DAVIDSON",
    "H.DAVIDSON": "HARLEY-DAVIDSON",
    "M.BENZ": "MERCEDES BENZ",
    "JTZ": "HAOJUE",
    "JTA": "SUZUKI",
    "JTA-SUZUKI": "SUZUKI",
    "MPOLO": "MARCOPOLO",
    "MMC": "MITSUBISHI",
    "CAOACHERY": "CHERY",
    "LR": "LAND ROVER",
    "RE": "ROYAL ENFIELD",
    "MOTO TRAXX": "TRAXX",
    "IVECOFIAT": "IVECO",
}

def extrair_montadora(texto):
    for marca in BRANDS:
        if texto.startswith(marca):
            return marca
    return None


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw DataFrame (from staging.detran_vehicle_raw) to norm schema.

    Steps (from scripts/normalize.py):
    1. Numeric conversion for ano and qtd, dropna
    2. qtd_barras, IMPORTADO, Marca Modelo s/ importado
    3. MARCA from first segment when qtd_barras >= 1
    4. Marca Modelo s/ marca (remove prefix before first /)
    5. ARTESANAL (MARCA starts with A.)
    6. Valid brands: groupby MARCA sum(Qtd) > 10, excluding ARTESANAL and IMPORTADO
    7. Keep only rows where MARCA is in valid brands
    8. Output: uf, marca, modelo, ano_fabricacao, frota, descricao_detran, id_raw

    Args:
        df: DataFrame from read_raw_data() (columns: uf, marca_modelo, ano_fabricacao_veiculo_crv, qtd_veiculos, id_raw, ...)

    Returns:
        DataFrame with columns: uf, marca, modelo, ano_fabricacao, frota, descricao_detran, id_raw
    """
    if df.empty:
        return pd.DataFrame(columns=["uf", "marca", "modelo", "ano_fabricacao", "frota", "descricao_detran", "id_raw"])

    # Use raw table column names; alias to script names for clarity
    work = df.copy()
    work[COL_UF] = work[RAW_UF]
    work[COL_MARCA_MODELO] = work[RAW_MARCA_MODELO].astype(str)
    work[COL_ANO] = pd.to_numeric(work[RAW_ANO], errors="coerce")
    work[COL_QTD] = pd.to_numeric(work[RAW_QTD], errors="coerce")

    work = work.dropna(subset=[COL_ANO, COL_QTD])
    if work.empty:
        return pd.DataFrame(columns=["uf", "marca", "modelo", "ano_fabricacao", "frota", "descricao_detran", "id_raw"])

    work[COL_ANO] = work[COL_ANO].astype(int)
    work[COL_QTD] = work[COL_QTD].astype(int)

    work["qtd_barras"] = work[COL_MARCA_MODELO].str.count("/")
    work["IMPORTADO"] = work[COL_MARCA_MODELO].str.startswith("I/", na=False)
    work["Marca Modelo s/ importado"] = work[COL_MARCA_MODELO].str.removeprefix("I/")

    work.loc[work["qtd_barras"] >= 1, "MARCA"] = (
        work.loc[work["qtd_barras"] >= 1, "Marca Modelo s/ importado"]
        .str.split("/")
        .str[0]
    )
    
    work["Marca Modelo s/ marca"] = (
        work["Marca Modelo s/ importado"]
        .str.replace(r"^[^/]+/", "", regex=True)
    )

    mask_importados = work['IMPORTADO']

    work.loc[mask_importados, 'MARCA'] = (
        work.loc[mask_importados, 'Marca Modelo s/ marca']
        .apply(extrair_montadora)
    )

    work.loc[mask_importados, 'Marca Modelo s/ marca'] = (
        work.loc[mask_importados]
        .apply(
            lambda row: re.sub(
                r'^' + re.escape(row['MARCA']) + r'\s*',
                '',
                row['Marca Modelo s/ marca']
            ) if pd.notna(row['MARCA']) else row['Marca Modelo s/ marca'],
            axis=1
        )
    )
    
    mask_imp = work['MARCA'] == 'IMP'
    work.loc[mask_imp, 'IMPORTADO'] = True
    work.loc[mask_imp, 'MARCA'] = (
        work.loc[mask_imp, 'Marca Modelo s/ marca']
        .apply(extrair_montadora)
    )
    work.loc[mask_imp, 'Marca Modelo s/ marca'] = (
        work.loc[mask_imp]
    )

    work.loc[mask_imp, 'Marca Modelo s/ marca'] = (
        work.loc[mask_imp]
        .apply(
            lambda row: re.sub(
                r'^' + re.escape(row['MARCA']) + r'\s*',
                '',
                row['Marca Modelo s/ marca']
            ) if pd.notna(row['MARCA']) else row['Marca Modelo s/ marca'],
            axis=1
        )
    )

    work["MARCA"] = work["MARCA"].map(lambda x: BRAND_NORMALIZATION.get(x, x) if pd.notna(x) else x)
    
    work["ARTESANAL"] = work["MARCA"].str.startswith("A.", na=False)

    resumo = (
        work[(~work["ARTESANAL"])]
        .groupby("MARCA", dropna=False)[COL_QTD]
        .sum()
    )
    marcas_validas = resumo[resumo > 10]
    work = work[work["MARCA"].isin(marcas_validas.index)]
    work[['MARCA', 'Marca Modelo s/ marca']] = work[['MARCA', 'Marca Modelo s/ marca']].replace('', np.nan)
    work = work.dropna(subset=['MARCA', 'Marca Modelo s/ marca'])

    if work.empty:
        return pd.DataFrame(columns=["uf", "marca", "modelo", "ano_fabricacao", "frota", "descricao_detran", "importado", "id_raw"])

    columns_map = {
        COL_UF: "uf",
        "MARCA": "marca",
        "Marca Modelo s/ marca": "modelo",
        COL_ANO: "ano_fabricacao",
        COL_QTD: "frota",
        COL_MARCA_MODELO: "descricao_detran",
        "IMPORTADO": "importado",
    }
    out_cols = [COL_UF, "MARCA", COL_MARCA_MODELO, "Marca Modelo s/ marca", COL_ANO, COL_QTD, "IMPORTADO"]
    df_final = work[out_cols + [RAW_ID]].rename(columns=columns_map)
    df_final = df_final.rename(columns={RAW_ID: "id_raw"})
    return df_final


def _norm_partition_name(report_period: int) -> str:
    """Return partition table name for norm (e.g. detran_vehicle_norm_202501)."""
    return f"detran_vehicle_norm_{report_period}"


def ensure_norm_table_exists(
    report_period: int,
    conn=None,
) -> Tuple[bool, bool]:
    """
    Ensure staging schema, detran_vehicle_norm partitioned table, and the
    partition for report_period exist.

    Args:
        report_period: Report period YYYYMM (e.g. 202501)
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

        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = 'staging'
        """)
        if cursor.fetchone() is None:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            conn.commit()
            schema_created = True

        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'staging'
            AND table_name = 'detran_vehicle_norm'
        """)
        if cursor.fetchone() is None:
            sql_file = Path(__file__).parent.parent / "sql" / "staging" / "03_create_norm_table.sql"
            execute_sql_file(str(sql_file), conn)
            table_created = True

        part_name = _norm_partition_name(report_period)
        cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'staging' AND tablename = %s
        """, (part_name,))
        if cursor.fetchone() is None:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS staging." + part_name + " PARTITION OF staging.detran_vehicle_norm FOR VALUES IN (%s)",
                (report_period,),
            )
            conn.commit()

        cursor.close()
        return schema_created, table_created

    finally:
        if close_conn:
            conn.close()


def read_raw_data(conn=None, report_period: Optional[int] = None) -> pd.DataFrame:
    """
    Read raw data from staging.detran_vehicle_raw (optionally for one report_period).

    Args:
        conn: Database connection (if None, creates new)
        report_period: If set, only rows for this period (YYYYMM) are returned.

    Returns:
        DataFrame with raw data (columns: id_raw, uf, marca_modelo, ano_fabricacao_veiculo_crv, qtd_veiculos, ...)
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        query = "SELECT id_raw, uf, marca_modelo, ano_fabricacao_veiculo_crv, qtd_veiculos FROM staging.detran_vehicle_raw"
        if report_period is not None:
            query += " WHERE report_period = %s"
            df = pd.read_sql(query, conn, params=(report_period,))
        else:
            df = pd.read_sql(query, conn)
        return df
    finally:
        if close_conn:
            conn.close()


def read_norm_data(conn=None, report_period: Optional[int] = None) -> pd.DataFrame:
    """
    Read normalized data from staging.detran_vehicle_norm (optionally for one report_period).

    Args:
        conn: Database connection (if None, creates new)
        report_period: If set, only rows for this period (YYYYMM) are returned.

    Returns:
        DataFrame with normalized data (columns: uf, marca, modelo, ano_fabricacao, frota, descricao_detran, id_raw, ...)
    """
    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        query = "SELECT * FROM staging.detran_vehicle_norm"
        if report_period is not None:
            query += " WHERE report_period = %s"
            df = pd.read_sql(query, conn, params=(report_period,))
        else:
            df = pd.read_sql(query, conn)
        return df
    finally:
        if close_conn:
            conn.close()


def load_normalized_to_staging(
    df: pd.DataFrame,
    report_period: int,
    table_name: str = "staging.detran_vehicle_norm",
    conn: Optional = None,
) -> int:
    """
    Load normalized DataFrame into staging.detran_vehicle_norm (partition for report_period).

    Uses INSERT only. Caller should truncate the partition before load if reprocessing.

    Args:
        df: DataFrame with columns uf, marca, modelo, ano_fabricacao, frota, descricao_detran, id_raw, importado
        report_period: Report period YYYYMM (e.g. 202501)
        table_name: Target table name (parent partitioned table)
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

        columns = ["report_period", "uf", "marca", "modelo", "ano_fabricacao", "frota", "descricao_detran", "id_raw", "importado"]
        values = []
        for _, row in df.iterrows():
            row_values = [report_period]
            for col in ["uf", "marca", "modelo", "ano_fabricacao", "frota", "descricao_detran", "id_raw", "importado"]:
                val = row.get(col)
                if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
                    row_values.append(None)
                else:
                    row_values.append(val)
            values.append(tuple(row_values))

        columns_str = ", ".join(columns)
        insert_template = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"

        cursor = conn.cursor()
        try:
            execute_values(
                cursor,
                insert_template,
                values,
                page_size=1000,
            )
            conn.commit()
            return len(values)
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error inserting normalized data: {str(e)}") from e
        finally:
            cursor.close()

    finally:
        if close_conn:
            conn.close()

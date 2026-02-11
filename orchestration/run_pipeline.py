"""
Main orchestration script - Single point of execution.

PHASE 1: Extract + Load Raw
- Extract from CSV (DETRAN vehicle fleet), load to staging.detran_vehicle_raw

# PHASE 2: Normalize + Load to staging.detran_vehicle_norm (commented out)
# - Read raw, normalize, load to staging.detran_vehicle_norm

# PHASE 3: Core (commented out)
# - Ensure core schema and tables (dim_veiculo, audit_dim_veiculo, trigger)
# - Add hash_veiculo to norm data, upsert into core.dim_veiculo (audit via trigger)

TODO: Phase 4 - Validate + Analytics
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.extract import read_csv_file, validate_csv_structure, list_csv_files
from pipeline.load import load_raw_data, ensure_staging_table_exists, truncate_staging_table
# from pipeline.normalize import (
#     ensure_norm_table_exists,
#     read_raw_data,
#     normalize_dataframe,
#     load_normalized_to_staging,
# )
# from pipeline.transform import (
#     ensure_core_tables_exist,
#     add_hash_to_norm_df,
#     upsert_dim_veiculo,
# )
from pipeline.utils import setup_logging
import os
from dotenv import load_dotenv

load_dotenv()


def main():
    """
    Main pipeline execution - Phase 1 only: Extract from CSV, Load Raw.

    Configuration:
    - CSV file: First .csv file found in data/input/ directory (DETRAN format, semicolon-separated)
    - Columns: UF, Marca Modelo, Ano Fabricação Veículo CRV, Qtd. Veículos
    """
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger = setup_logging(log_level=log_level)

    logger.info("=" * 80)
    logger.info("DETRAN VEHICLE DW PIPELINE - Phase 1: Extract from CSV, Load Raw")
    logger.info("=" * 80)

    try:
        # Get CSV file from data/input directory
        input_dir = "data/input"
        csv_files = list_csv_files(input_dir)

        if not csv_files:
            logger.error(f"No CSV files found in {input_dir}")
            logger.error("Please place DETRAN CSV file in data/input/ directory")
            sys.exit(1)

        if len(csv_files) > 1:
            logger.warning(f"Multiple CSV files found. Using first: {csv_files[0]}")

        csv_file = Path(csv_files[0])
        logger.info(f"CSV file: {csv_file.name} (from {input_dir}/)")

        # Ensure staging schema and raw table exist
        logger.info("Step 0: Ensuring staging schema and raw table exist...")
        schema_created, table_created = ensure_staging_table_exists()
        if schema_created:
            logger.info("Created staging schema")
        if table_created:
            logger.info("Created staging.detran_vehicle_raw table")
        if not schema_created and not table_created:
            logger.info("Schema and table already exist")

        # Extract: Read CSV (semicolon-separated, DETRAN format)
        logger.info("Step 1: Extracting data from CSV...")
        df = read_csv_file(str(csv_file))
        logger.info(f"Extracted {len(df)} rows and {len(df.columns)} columns")

        # Validate CSV structure
        logger.info("Step 2: Validating CSV structure...")
        validate_csv_structure(df)
        logger.info("CSV structure validated successfully")

        # Truncate staging table before loading (history is kept in CSV files)
        logger.info("Step 2.5: Truncating staging table (clearing previous data)...")
        truncate_staging_table()
        logger.info("Staging table truncated successfully")

        # Load: Insert into staging.detran_vehicle_raw
        logger.info("Step 3: Loading data into staging.detran_vehicle_raw...")
        rows_inserted = load_raw_data(
            df,
            source_file=str(csv_file.absolute()),
            conn=None  # Will create new connection
        )
        logger.info(f"Successfully inserted {rows_inserted} rows into staging.detran_vehicle_raw")

        # ========================================================================
        # PHASE 2: NORMALIZE (commented out)
        # ========================================================================
        # logger.info("")
        # logger.info("=" * 80)
        # logger.info("PHASE 2: Normalize + Load to staging.detran_vehicle_norm")
        # logger.info("=" * 80)
        # logger.info("Step 4: Ensuring staging.detran_vehicle_norm table exists...")
        # schema_created, table_created = ensure_norm_table_exists()
        # ...
        # logger.info("Step 5: Reading data from staging.detran_vehicle_raw...")
        # df_raw = read_raw_data()
        # ...
        # logger.info("Step 6: Normalizing data...")
        # df_norm = normalize_dataframe(df_raw)
        # ...
        # logger.info("Step 7: Loading normalized data into staging.detran_vehicle_norm...")
        # rows_norm_inserted = load_normalized_to_staging(df_norm)
        # ...

        # ========================================================================
        # PHASE 3: CORE (commented out)
        # ========================================================================
        # logger.info("")
        # logger.info("=" * 80)
        # logger.info("PHASE 3: Transform to core.dim_veiculo (hash + upsert + audit)")
        # logger.info("=" * 80)
        # logger.info("Step 8: Ensuring core schema and tables exist...")
        # core_schema_created, core_tables_created = ensure_core_tables_exist()
        # ...
        # logger.info("Step 9: Adding hash_veiculo and upserting into core.dim_veiculo...")
        # df_core = add_hash_to_norm_df(df_norm)
        # result = upsert_dim_veiculo(df_core)
        # ...

        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("PHASE 1 COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Phase 1 - Rows processed: {rows_inserted}")
        logger.info(f"Phase 1 - Data available in: staging.detran_vehicle_raw")
        logger.info("")
        logger.info("Phases 2 (normalize) and 3 (core) are currently commented out.")
        logger.info("Next steps (when enabled): Phase 2 normalização, Phase 3 core, Phase 4 analytics.")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

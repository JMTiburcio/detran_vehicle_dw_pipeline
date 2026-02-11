"""
Main orchestration script - Single point of execution.

PHASE 1: Extract + Load Raw
- Extract from CSV (DETRAN vehicle fleet), load to staging.detran_vehicle_raw

PHASE 2: Normalize + Load to staging.detran_vehicle_norm
- Read from staging.detran_vehicle_raw, normalize (marca/modelo, valid brands), load to staging.detran_vehicle_norm

# PHASE 3: Core (commented out)
# - Ensure core schema and tables (dim_veiculo, audit_dim_veiculo, trigger)
# - Add hash_veiculo to norm data, upsert into core.dim_veiculo (audit via trigger)

TODO: Phase 4 - Validate + Analytics
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestration.pipeline_args import parse_pipeline_args, PHASE_RAW, PHASE_NORMALIZE
from pipeline.extract import read_csv_file, validate_csv_structure, list_csv_files
from pipeline.load import load_raw_data, ensure_staging_table_exists, truncate_staging_table
from pipeline.normalize import (
    ensure_norm_table_exists,
    read_raw_data,
    normalize_dataframe,
    load_normalized_to_staging,
)
# from pipeline.transform import (
#     ensure_core_tables_exist,
#     add_hash_to_norm_df,
#     upsert_dim_veiculo,
# )
from pipeline.utils import setup_logging
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def main():
    """
    Main pipeline execution. Start phase is set via CLI (--start-from raw|normalize|core).
    """
    opts = parse_pipeline_args()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger = setup_logging(log_level=log_level)

    logger.info("=" * 80)
    logger.info("DETRAN VEHICLE DW PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Starting from phase: {opts.start_from_name}")

    rows_inserted = None
    df_raw = pd.DataFrame()
    df_norm = pd.DataFrame()
    rows_norm_inserted = 0

    try:
        # ---------------------------------------------------------------------
        # PHASE 1: Extract + Load Raw
        # ---------------------------------------------------------------------
        if opts.start_from <= PHASE_RAW:
            input_dir = "data/input"
            csv_files = list_csv_files(input_dir)
            if not csv_files:
                logger.error(f"No CSV files found in {input_dir}")
                sys.exit(1)
            if len(csv_files) > 1:
                logger.warning(f"Multiple CSV files found. Using first: {csv_files[0]}")
            csv_file = Path(csv_files[0])
            logger.info(f"CSV file: {csv_file.name} (from {input_dir}/)")

            logger.info("Step 0: Ensuring staging schema and raw table exist...")
            schema_created, table_created = ensure_staging_table_exists()
            if schema_created:
                logger.info("Created staging schema")
            if table_created:
                logger.info("Created staging.detran_vehicle_raw table")
            if not schema_created and not table_created:
                logger.info("Schema and table already exist")

            logger.info("Step 1: Extracting data from CSV...")
            df = read_csv_file(str(csv_file))
            logger.info(f"Extracted {len(df)} rows and {len(df.columns)} columns")
            logger.info("Step 2: Validating CSV structure...")
            validate_csv_structure(df)
            logger.info("Step 2.5: Truncating staging table...")
            truncate_staging_table()
            logger.info("Step 3: Loading data into staging.detran_vehicle_raw...")
            rows_inserted = load_raw_data(
                df, source_file=str(csv_file.absolute()), conn=None
            )
            logger.info(f"Inserted {rows_inserted} rows into staging.detran_vehicle_raw")

        # ---------------------------------------------------------------------
        # PHASE 2: Normalize (read from raw, load to norm)
        # ---------------------------------------------------------------------
        if opts.start_from <= PHASE_NORMALIZE:
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 2: Normalize + Load to staging.detran_vehicle_norm")
            logger.info("=" * 80)

            logger.info("Step 4: Ensuring staging.detran_vehicle_norm table exists...")
            schema_created, table_created = ensure_norm_table_exists()
            if schema_created:
                logger.info("Created staging schema")
            if table_created:
                logger.info("Created staging.detran_vehicle_norm table")
            if not schema_created and not table_created:
                logger.info("Schema and table already exist")

            logger.info("Step 5: Reading data from staging.detran_vehicle_raw...")
            df_raw = read_raw_data()
            logger.info(f"Read {len(df_raw)} rows from staging.detran_vehicle_raw")

            if len(df_raw) == 0:
                logger.warning("No data in staging.detran_vehicle_raw. Skipping normalization.")
            else:
                logger.info("Step 6: Normalizing data (marca/modelo split, valid brands)...")
                df_norm = normalize_dataframe(df_raw)
                logger.info(f"Normalized: {len(df_norm)} rows")
                if len(df_norm) > 0:
                    logger.info("Step 6.5: Truncating staging.detran_vehicle_norm...")
                    truncate_staging_table(table_name="staging.detran_vehicle_norm")
                    logger.info("Step 7: Loading normalized data into staging.detran_vehicle_norm...")
                    rows_norm_inserted = load_normalized_to_staging(df_norm)
                    logger.info(f"Inserted {rows_norm_inserted} rows into staging.detran_vehicle_norm")

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
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        if rows_inserted is not None:
            logger.info(f"Phase 1 - Rows loaded to raw: {rows_inserted} (staging.detran_vehicle_raw)")
        if len(df_norm) > 0:
            logger.info(f"Phase 2 - Rows loaded to norm: {rows_norm_inserted} (staging.detran_vehicle_norm)")
        logger.info("")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

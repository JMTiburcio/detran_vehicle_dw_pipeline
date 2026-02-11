"""
Main orchestration script - Single point of execution.

PHASE 1: Extract + Load Raw
- Extract from Excel, load to staging.fraga_vehicle_raw

PHASE 2: Normalize + Load to staging.fraga_vehicle_norm
- Read raw, normalize, explode by year, load to staging.fraga_vehicle_norm

PHASE 3: Core
- Ensure core schema and tables (dim_veiculo, audit_dim_veiculo, trigger)
- Add hash_veiculo to norm data, upsert into core.dim_veiculo (audit via trigger)

TODO: Phase 4 - Validate + Analytics
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.extract import read_excel_file, validate_excel_structure, list_excel_files
from pipeline.load import load_raw_data, ensure_staging_table_exists, truncate_staging_table
from pipeline.normalize import (
    ensure_norm_table_exists,
    read_raw_data,
    normalize_dataframe,
    load_normalized_to_staging,
)
from pipeline.transform import (
    ensure_core_tables_exist,
    add_hash_to_norm_df,
    upsert_dim_veiculo,
)
from pipeline.utils import setup_logging
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def main():
    """
    Main pipeline execution function - Phase 1 & 2: Extract, Load Raw, Normalize.
    
    Configuration:
    - Excel file: First .xlsx or .xls file found in data/input/ directory
    - Sheet: First sheet in the Excel file (auto-detected)
    - Header: First row (auto-detected by pandas)
    """
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger = setup_logging(log_level=log_level)
    
    logger.info("=" * 80)
    logger.info("FRAGA VEHICLE DW PIPELINE - Phase 1, 2 & 3: Extract, Load Raw, Normalize, Core")
    logger.info("=" * 80)
    
    try:
        # Get Excel file from data/input directory (always same path)
        input_dir = "data/input"
        excel_files = list_excel_files(input_dir)
        
        if not excel_files:
            logger.error(f"No Excel files found in {input_dir}")
            logger.error("Please place Excel file in data/input/ directory")
            sys.exit(1)
        
        if len(excel_files) > 1:
            logger.warning(f"Multiple Excel files found. Using first: {excel_files[0]}")
        
        excel_file = Path(excel_files[0])
        logger.info(f"Excel file: {excel_file.name} (from {input_dir}/)")
        
        # Ensure staging schema and table exist
        logger.info("Step 0: Ensuring staging schema and table exist...")
        schema_created, table_created = ensure_staging_table_exists()
        if schema_created:
            logger.info("Created staging schema")
        if table_created:
            logger.info("Created staging.fraga_vehicle_raw table")
        if not schema_created and not table_created:
            logger.info("Schema and table already exist")
        
        # Extract: Read Excel (first sheet, header auto-detected)
        logger.info("Step 1: Extracting data from Excel...")
        df = read_excel_file(
            str(excel_file)
        )
        logger.info(f"Extracted {len(df)} rows and {len(df.columns)} columns")
        
        # Validate Excel structure
        logger.info("Step 2: Validating Excel structure...")
        validate_excel_structure(df)
        logger.info("Excel structure validated successfully")
        
        # Truncate staging table before loading (history is kept in Excel files)
        logger.info("Step 2.5: Truncating staging table (clearing previous data)...")
        truncate_staging_table()
        logger.info("Staging table truncated successfully")
        
        # Load: Insert into staging.fraga_vehicle_raw
        logger.info("Step 3: Loading data into staging.fraga_vehicle_raw...")
        rows_inserted = load_raw_data(
            df,
            source_file=str(excel_file.absolute()),
            conn=None  # Will create new connection
        )
        logger.info(f"Successfully inserted {rows_inserted} rows into staging.fraga_vehicle_raw")
        
        # ========================================================================
        # PHASE 2: NORMALIZE
        # ========================================================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("PHASE 2: Normalize + Load to staging.fraga_vehicle_norm")
        logger.info("=" * 80)
        
        # Ensure norm table exists
        logger.info("Step 4: Ensuring staging.fraga_vehicle_norm table exists...")
        schema_created, table_created = ensure_norm_table_exists()
        if schema_created:
            logger.info("Created staging schema")
        if table_created:
            logger.info("Created staging.fraga_vehicle_norm table")
        if not schema_created and not table_created:
            logger.info("Schema and table already exist")
        
        # Read raw data
        logger.info("Step 5: Reading data from staging.fraga_vehicle_raw...")
        df_raw = read_raw_data()
        logger.info(f"Read {len(df_raw)} rows from staging.fraga_vehicle_raw")
        
        rows_norm_inserted = 0
        df_norm = pd.DataFrame()
        
        if len(df_raw) == 0:
            logger.warning("No data found in staging.fraga_vehicle_raw. Skipping normalization.")
        else:
            # Normalize data
            logger.info("Step 6: Normalizing data (text normalization, year explosion)...")
            df_norm = normalize_dataframe(df_raw)
            logger.info(f"Normalized data: {len(df_norm)} rows (after year explosion)")
            
            if len(df_norm) == 0:
                logger.warning("No normalized rows generated. Check data_inicio values.")
            else:
                # Truncate norm table before loading (history is kept in raw table)
                logger.info("Step 6.5: Truncating staging.fraga_vehicle_norm table (clearing previous data)...")
                truncate_staging_table(table_name="staging.fraga_vehicle_norm")
                logger.info("Norm table truncated successfully")
                
                # Load normalized data
                logger.info("Step 7: Loading normalized data into staging.fraga_vehicle_norm...")
                rows_norm_inserted = load_normalized_to_staging(df_norm)
                logger.info(f"Successfully inserted {rows_norm_inserted} rows into staging.fraga_vehicle_norm")

                # ========================================================================
                # PHASE 3: CORE (transform norm -> core.dim_veiculo with hash + audit)
                # ========================================================================
                logger.info("")
                logger.info("=" * 80)
                logger.info("PHASE 3: Transform to core.dim_veiculo (hash + upsert + audit)")
                logger.info("=" * 80)
                logger.info("Step 8: Ensuring core schema and tables exist...")
                core_schema_created, core_tables_created = ensure_core_tables_exist()
                if core_schema_created:
                    logger.info("Created core schema")
                if core_tables_created:
                    logger.info("Created core.dim_veiculo and/or core.audit_dim_veiculo and trigger")
                if not core_schema_created and not core_tables_created:
                    logger.info("Core schema and tables already exist")
                logger.info("Step 9: Adding hash_veiculo and upserting into core.dim_veiculo...")
                df_core = add_hash_to_norm_df(df_norm)
                result = upsert_dim_veiculo(df_core)
                logger.info(f"Successfully upserted {result['inserted']} rows into core.dim_veiculo (audit via trigger)")
        
        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("PHASE 1, 2 & 3 COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Phase 1 - Rows processed: {rows_inserted}")
        logger.info(f"Phase 1 - Data available in: staging.fraga_vehicle_raw")
        if len(df_raw) > 0 and len(df_norm) > 0:
            logger.info(f"Phase 2 - Rows processed: {rows_norm_inserted}")
            logger.info(f"Phase 2 - Data available in: staging.fraga_vehicle_norm")
            logger.info(f"Phase 2 - Year explosion: {len(df_raw)} raw rows -> {len(df_norm)} normalized rows")
            logger.info(f"Phase 3 - Data available in: core.dim_veiculo (history in core.audit_dim_veiculo)")
        logger.info("")
        logger.info("Next steps (not yet implemented):")
        logger.info("  - Phase 4: Validate and create analytics views")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

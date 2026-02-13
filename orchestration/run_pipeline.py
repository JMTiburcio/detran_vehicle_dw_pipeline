"""
Main orchestration script - Single point of execution.

PHASE 1: Extract + Load Raw
- Extract from CSV (DETRAN vehicle fleet), load to staging.detran_vehicle_raw

PHASE 2: Normalize + Load to staging.detran_vehicle_norm
- Read from staging.detran_vehicle_raw, normalize (marca/modelo, valid brands), load to staging.detran_vehicle_norm

PHASE 3: Core (dimensional model)
- Ensure core schema and tables (dim_veiculo_detran, fato_frota_uf)
- Extract unique vehicles from norm, upsert into dim_veiculo_detran
- Extract frota facts (uf, frota per vehicle), upsert into fato_frota_uf

TODO: Phase 4 - Validate + Analytics
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestration.pipeline_args import parse_pipeline_args, PHASE_RAW, PHASE_NORMALIZE, PHASE_CORE
from pipeline.extract import read_csv_file, validate_csv_structure, list_csv_files
from pipeline.load import load_raw_data, ensure_staging_table_exists, truncate_staging_table
from pipeline.normalize import (
    ensure_norm_table_exists,
    read_raw_data,
    read_norm_data,
    normalize_dataframe,
    load_normalized_to_staging,
)
from pipeline.transform import (
    ensure_core_detran_tables_exist,
    truncate_core_tables,
    prepare_dim_veiculo_from_norm,
    upsert_dim_veiculo_detran,
    get_id_veiculo_from_hashes,
    upsert_fato_frota_uf,
)
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
    logger.info(f"Starting from phase: {opts.start_from_name}, stopping at: {opts.stop_at_name}")

    rows_inserted = None
    df_raw = pd.DataFrame()
    df_norm = pd.DataFrame()
    rows_norm_inserted = 0
    rows_dim_upserted = 0
    rows_fato_upserted = 0

    try:
        # ---------------------------------------------------------------------
        # PHASE 1: Extract + Load Raw
        # ---------------------------------------------------------------------
        if opts.start_from <= PHASE_RAW and opts.stop_at >= PHASE_RAW:
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
        if opts.start_from <= PHASE_NORMALIZE and opts.stop_at >= PHASE_NORMALIZE:
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

        # ---------------------------------------------------------------------
        # PHASE 3: Core (dimensional: dim_veiculo + fato_frota)
        # ---------------------------------------------------------------------
        if opts.start_from <= PHASE_CORE and opts.stop_at >= PHASE_CORE:
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 3: Transform to core (dim_veiculo_detran + fato_frota_uf)")
            logger.info("=" * 80)

            logger.info("Step 8: Ensuring core schema and tables exist...")
            core_schema_created, core_tables_created = ensure_core_detran_tables_exist()
            if core_schema_created:
                logger.info("Created core schema")
            if core_tables_created:
                logger.info("Created dim_veiculo_detran, fato_frota_uf")
            if not core_schema_created and not core_tables_created:
                logger.info("Core schema and tables already exist")

            logger.info("Step 8.5: Truncating core tables (dim_veiculo_detran, fato_frota_uf)...")
            truncate_core_tables()
            logger.info("Core tables truncated")

            # Read norm data: if we ran Phase 2, use df_norm in memory; else read from DB
            if opts.start_from <= PHASE_NORMALIZE and len(df_norm) > 0:
                df_for_core = df_norm
                logger.info(f"Step 9: Using {len(df_for_core)} rows from Phase 2 (in memory)")
            else:
                logger.info("Step 9: Reading data from staging.detran_vehicle_norm...")
                df_for_core = read_norm_data()
                logger.info(f"Read {len(df_for_core)} rows from staging.detran_vehicle_norm")

            if len(df_for_core) == 0:
                logger.warning("No data in norm. Skipping core upsert.")
            else:
                # Step 10: Prepare and upsert dim_veiculo (unique vehicles)
                logger.info("Step 10: Preparing dim_veiculo (unique marca+modelo+ano+descricao)...")
                df_dim = prepare_dim_veiculo_from_norm(df_for_core)
                logger.info(f"Unique vehicles: {len(df_dim)}")

                logger.info("Step 11: Upserting into core.dim_veiculo_detran...")
                rows_dim_upserted = upsert_dim_veiculo_detran(df_dim)
                logger.info(f"Upserted {rows_dim_upserted} vehicles into core.dim_veiculo_detran")

                # Step 12: Get id_veiculo for each hash, prepare fato_frota
                logger.info("Step 12: Querying id_veiculo from dim_veiculo_detran...")
                hashes = df_dim["hash_veiculo"].unique().tolist()
                df_id_map = get_id_veiculo_from_hashes(hashes)
                logger.info(f"Retrieved {len(df_id_map)} id_veiculo mappings")

                # Add hash_veiculo to df_for_core (for merge)
                from pipeline.transform import generate_hash_veiculo_detran
                df_for_core["hash_veiculo"] = df_for_core.apply(
                    lambda r: generate_hash_veiculo_detran(r["marca"], r["modelo"], r["ano_fabricacao"]),
                    axis=1
                )

                # Merge id_veiculo into df_for_core
                df_for_core = df_for_core.merge(df_id_map, on="hash_veiculo", how="left")

                # Build fato DataFrame: id_veiculo, uf, frota, id_raw
                df_fato = df_for_core[["id_veiculo", "uf", "frota", "id_raw"]].copy()
                logger.info(f"Step 13: Prepared {len(df_fato)} fato_frota rows")

                logger.info("Step 14: Upserting into core.fato_frota_uf...")
                rows_fato_upserted = upsert_fato_frota_uf(df_fato)
                logger.info(f"Upserted {rows_fato_upserted} rows into core.fato_frota_uf")

        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        if rows_inserted is not None:
            logger.info(f"Phase 1 - Rows loaded to raw: {rows_inserted} (staging.detran_vehicle_raw)")
        if rows_norm_inserted > 0:
            logger.info(f"Phase 2 - Rows loaded to norm: {rows_norm_inserted} (staging.detran_vehicle_norm)")
        if rows_dim_upserted > 0:
            logger.info(f"Phase 3 - Dim vehicles upserted: {rows_dim_upserted} (core.dim_veiculo_detran)")
        if rows_fato_upserted > 0:
            logger.info(f"Phase 3 - Fato frota upserted: {rows_fato_upserted} (core.fato_frota_uf)")
        logger.info("")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

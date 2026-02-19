-- Create partitioned raw table for csv data (by report_period YYYYMM)
-- Partitions are created on demand in Python (e.g. detran_vehicle_raw_202501)
-- Pipeline stage: Extract + Load Raw (Phase 1)

CREATE TABLE IF NOT EXISTS staging.detran_vehicle_raw (
    report_period INTEGER NOT NULL,
    id_raw SERIAL,
    -- Original columns from detran csv (normalized names, Portuguese)
    uf VARCHAR(100),
    marca_modelo VARCHAR(100),
    ano_fabricacao_veiculo_crv VARCHAR(100),
    qtd_veiculos VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    csv_row INTEGER,
    PRIMARY KEY (report_period, id_raw)
) PARTITION BY LIST (report_period);

COMMENT ON TABLE staging.detran_vehicle_raw IS 'Raw data from detran csv files, partitioned by report period (YYYYMM). Partitions created on demand.'

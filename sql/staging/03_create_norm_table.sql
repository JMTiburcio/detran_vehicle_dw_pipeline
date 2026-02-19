-- Create partitioned normalized table (by report_period YYYYMM)
-- Partitions are created on demand in Python (e.g. detran_vehicle_norm_202501)
-- Pipeline stage: Normalize (Phase 2)

CREATE TABLE IF NOT EXISTS staging.detran_vehicle_norm (
    report_period INTEGER NOT NULL,
    id_norm SERIAL,
    uf VARCHAR(100),
    marca VARCHAR(255),
    modelo VARCHAR(255),
    ano_fabricacao INTEGER,
    frota INTEGER,
    descricao_detran VARCHAR(255),
    importado BOOLEAN,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_raw INTEGER NOT NULL,
    PRIMARY KEY (report_period, id_norm),
    FOREIGN KEY (report_period, id_raw) REFERENCES staging.detran_vehicle_raw(report_period, id_raw)
) PARTITION BY LIST (report_period);

COMMENT ON TABLE staging.detran_vehicle_norm IS 'Normalized DETRAN vehicle fleet, partitioned by report period (YYYYMM). Partitions created on demand.';

CREATE INDEX IF NOT EXISTS idx_norm_detran_report_period_id_raw ON staging.detran_vehicle_norm(report_period, id_raw);

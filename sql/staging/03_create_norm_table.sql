-- Create normalized table for DETRAN vehicle fleet
-- Output of normalization: UF, marca, modelo, ano_fabricacao, frota, descricao_detran
-- Pipeline stage: Normalize (Phase 2)

CREATE TABLE IF NOT EXISTS staging.detran_vehicle_norm (
    id_norm SERIAL PRIMARY KEY,
    uf VARCHAR(100),
    marca VARCHAR(255),
    modelo VARCHAR(255),
    ano_fabricacao INTEGER,
    frota INTEGER,
    descricao_detran VARCHAR(255),
    -- Metadata
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_raw INTEGER REFERENCES staging.detran_vehicle_raw(id_raw)
);

COMMENT ON TABLE staging.detran_vehicle_norm IS 'Normalized DETRAN vehicle fleet: marca/modelo split, valid brands (sum > 10), no artesanal/importado. Loaded by INSERT only after truncate.';

CREATE INDEX IF NOT EXISTS idx_norm_detran_id_raw ON staging.detran_vehicle_norm(id_raw);

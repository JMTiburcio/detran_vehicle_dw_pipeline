-- Create dimension table for DETRAN vehicles
-- Columns from staging.detran_vehicle_norm plus hash_veiculo for unique identification.
-- SCD Type 1: Current state in table; history in core.audit_dim_detran_veiculo (via trigger).
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.dim_detran_veiculo (
    id_veiculo SERIAL PRIMARY KEY,
    hash_veiculo VARCHAR(64) NOT NULL UNIQUE,

    -- Columns from norm table (staging.detran_vehicle_norm)
    uf VARCHAR(100),
    marca VARCHAR(255),
    modelo VARCHAR(255),
    ano_fabricacao INTEGER,
    frota INTEGER,
    descricao_detran VARCHAR(255),

    -- Traceability: reference to staging.detran_vehicle_norm
    id_raw INTEGER,

    -- Core metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE core.dim_detran_veiculo IS 'Dimension table for DETRAN vehicles - unique by uf+marca+modelo+ano_fabricacao (hash). History in audit_dim_detran_veiculo.';
COMMENT ON COLUMN core.dim_detran_veiculo.id_veiculo IS 'Surrogate key (technical PK)';
COMMENT ON COLUMN core.dim_detran_veiculo.hash_veiculo IS 'Natural business key: SHA256 of uf|marca|modelo|ano_fabricacao';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dim_detran_hash_veiculo ON core.dim_detran_veiculo(hash_veiculo);
CREATE INDEX IF NOT EXISTS idx_dim_detran_uf ON core.dim_detran_veiculo(uf);
CREATE INDEX IF NOT EXISTS idx_dim_detran_marca ON core.dim_detran_veiculo(marca);
CREATE INDEX IF NOT EXISTS idx_dim_detran_ano ON core.dim_detran_veiculo(ano_fabricacao);
CREATE INDEX IF NOT EXISTS idx_dim_detran_marca_modelo ON core.dim_detran_veiculo(marca, modelo);

-- Create dimension table for DETRAN vehicles (marca, modelo, ano_fabricacao, descricao_detran)
-- One unique vehicle per combination of marca + modelo + ano_fabricacao
-- SCD Type 1: Current state; history in audit_dim_veiculo_detran
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.dim_veiculo_detran (
    id_veiculo SERIAL PRIMARY KEY,
    hash_veiculo VARCHAR(64) NOT NULL UNIQUE,

    -- Vehicle attributes
    marca VARCHAR(255) NOT NULL,
    modelo VARCHAR(255) NOT NULL,
    ano_fabricacao INTEGER NOT NULL,
    descricao_detran VARCHAR(255),

    -- Core metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE core.dim_veiculo_detran IS 'Dimension table for DETRAN vehicles - unique by marca+modelo+ano_fabricacao. History in audit_dim_veiculo_detran.';
COMMENT ON COLUMN core.dim_veiculo_detran.id_veiculo IS 'Surrogate key (technical PK)';
COMMENT ON COLUMN core.dim_veiculo_detran.hash_veiculo IS 'Natural business key: SHA256 of marca|modelo|ano_fabricacao';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dim_veiculo_marca ON core.dim_veiculo_detran(marca);
CREATE INDEX IF NOT EXISTS idx_dim_veiculo_ano ON core.dim_veiculo_detran(ano_fabricacao);
CREATE INDEX IF NOT EXISTS idx_dim_veiculo_marca_modelo ON core.dim_veiculo_detran(marca, modelo);

-- Create fact table for vehicle fleet by UF
-- Links dim_veiculo_detran (vehicle) with UF and fleet quantity
-- Unique constraint on (id_veiculo, uf)
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.fato_frota_uf (
    id_fato SERIAL PRIMARY KEY,
    id_veiculo INTEGER NOT NULL REFERENCES core.dim_veiculo_detran(id_veiculo),
    uf VARCHAR(100) NOT NULL,
    frota INTEGER NOT NULL,

    -- Traceability to staging
    id_raw INTEGER,

    -- Core metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Business key: one row per vehicle + UF combination
    UNIQUE(id_veiculo, uf)
);

COMMENT ON TABLE core.fato_frota_uf IS 'Fact table for vehicle fleet by UF - links dim_veiculo_detran with UF and fleet count. History in audit_fato_frota_uf.';
COMMENT ON COLUMN core.fato_frota_uf.id_fato IS 'Surrogate key (technical PK)';
COMMENT ON COLUMN core.fato_frota_uf.id_veiculo IS 'Foreign key to dim_veiculo_detran';
COMMENT ON COLUMN core.fato_frota_uf.uf IS 'Brazilian state (UF)';
COMMENT ON COLUMN core.fato_frota_uf.frota IS 'Vehicle fleet count in this UF';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fato_frota_id_veiculo ON core.fato_frota_uf(id_veiculo);
CREATE INDEX IF NOT EXISTS idx_fato_frota_uf ON core.fato_frota_uf(uf);

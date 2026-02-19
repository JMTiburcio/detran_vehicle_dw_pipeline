-- Create partitioned fact table for vehicle fleet by UF (by report_period YYYYMM)
-- Partitions are created on demand in Python (e.g. fato_frota_uf_202501)
-- Links dim_veiculo_detran (vehicle) with UF and fleet quantity
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.fato_frota_uf (
    report_period INTEGER NOT NULL,
    id_fato SERIAL,
    id_veiculo INTEGER NOT NULL REFERENCES core.dim_veiculo_detran(id_veiculo),
    uf VARCHAR(100) NOT NULL,
    frota INTEGER NOT NULL,
    id_raw INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_period, id_fato),
    UNIQUE (report_period, id_veiculo, uf)
) PARTITION BY LIST (report_period);

COMMENT ON TABLE core.fato_frota_uf IS 'Fact table for vehicle fleet by UF, partitioned by report period (YYYYMM). Partitions created on demand.';
COMMENT ON COLUMN core.fato_frota_uf.report_period IS 'Report period YYYYMM (e.g. 202501)';
COMMENT ON COLUMN core.fato_frota_uf.id_fato IS 'Surrogate key (technical PK)';
COMMENT ON COLUMN core.fato_frota_uf.id_veiculo IS 'Foreign key to dim_veiculo_detran';
COMMENT ON COLUMN core.fato_frota_uf.uf IS 'Brazilian state (UF)';
COMMENT ON COLUMN core.fato_frota_uf.frota IS 'Vehicle fleet count in this UF';

CREATE INDEX IF NOT EXISTS idx_fato_frota_report_period ON core.fato_frota_uf(report_period);
CREATE INDEX IF NOT EXISTS idx_fato_frota_id_veiculo ON core.fato_frota_uf(id_veiculo);
CREATE INDEX IF NOT EXISTS idx_fato_frota_uf ON core.fato_frota_uf(uf);

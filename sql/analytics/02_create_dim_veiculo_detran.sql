-- Analytics replica of core.dim_veiculo_detran (same structure, no FK)
-- Pipeline stage: Analytics (Phase 4) - populated by refresh_analytics_from_core

CREATE TABLE IF NOT EXISTS analytics.dim_veiculo_detran (
    id_veiculo SERIAL PRIMARY KEY,
    hash_veiculo VARCHAR(64) NOT NULL UNIQUE,
    marca VARCHAR(255) NOT NULL,
    modelo VARCHAR(255) NOT NULL,
    ano_fabricacao INTEGER NOT NULL,
    descricao_detran VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE analytics.dim_veiculo_detran IS 'Stable replica of core.dim_veiculo_detran for BI; updated by swap.';

CREATE INDEX IF NOT EXISTS idx_analytics_dim_veiculo_marca ON analytics.dim_veiculo_detran(marca);
CREATE INDEX IF NOT EXISTS idx_analytics_dim_veiculo_ano ON analytics.dim_veiculo_detran(ano_fabricacao);
CREATE INDEX IF NOT EXISTS idx_analytics_dim_veiculo_marca_modelo ON analytics.dim_veiculo_detran(marca, modelo);

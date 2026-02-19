-- Staging tables for swap: same structure as dim/fato, populated then renamed to final name
-- Used by refresh_analytics_from_core

CREATE TABLE IF NOT EXISTS analytics.dim_veiculo_detran_new (
    id_veiculo SERIAL PRIMARY KEY,
    hash_veiculo VARCHAR(64) NOT NULL UNIQUE,
    marca VARCHAR(255) NOT NULL,
    modelo VARCHAR(255) NOT NULL,
    ano_fabricacao INTEGER NOT NULL,
    descricao_detran VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.fato_frota_uf_new (
    report_period INTEGER NOT NULL,
    id_fato SERIAL,
    id_veiculo INTEGER NOT NULL,
    uf VARCHAR(100) NOT NULL,
    frota INTEGER NOT NULL,
    id_raw INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_period, id_fato),
    UNIQUE (report_period, id_veiculo, uf)
);

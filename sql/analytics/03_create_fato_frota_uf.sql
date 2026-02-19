-- Analytics replica of core.fato_frota_uf (single non-partitioned table, all report_periods)
-- No FK to core; id_veiculo is copied value only
-- Pipeline stage: Analytics (Phase 4) - populated by refresh_analytics_from_core

CREATE TABLE IF NOT EXISTS analytics.fato_frota_uf (
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

COMMENT ON TABLE analytics.fato_frota_uf IS 'Stable replica of core.fato_frota_uf for BI; single table, all report_periods; updated by swap.';

CREATE INDEX IF NOT EXISTS idx_analytics_fato_report_period ON analytics.fato_frota_uf(report_period);
CREATE INDEX IF NOT EXISTS idx_analytics_fato_id_veiculo ON analytics.fato_frota_uf(id_veiculo);
CREATE INDEX IF NOT EXISTS idx_analytics_fato_uf ON analytics.fato_frota_uf(uf);

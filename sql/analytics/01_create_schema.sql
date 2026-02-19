-- Create analytics schema
-- Stable replicas of core tables for BI consumption; updated by atomic swap

CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA analytics IS 'Stable replicas of core (dim_veiculo_detran, fato_frota_uf) for BI; updated by swap, no direct truncate';

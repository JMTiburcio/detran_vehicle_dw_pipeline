-- Create view for BI consumption
-- This view provides a clean interface to dim_veiculo
--
-- STATUS: NOT READY - View defined but depends on core.dim_veiculo which is not populated yet
-- Pipeline stage: Analytics (Phase 4)
-- TODO: This view will be usable once dim_veiculo is populated

CREATE OR REPLACE VIEW analytics.vw_dim_veiculo AS
SELECT 
    id_veiculo,
    hash_veiculo,
    codigo_fraga,
    marca,
    modelo,
    versao,
    motor,
    combustivel,
    transmissao,
    aspiracao,
    created_at,
    updated_at
FROM core.dim_veiculo;

COMMENT ON VIEW analytics.vw_dim_veiculo IS 'View for BI consumption - clean interface to vehicle dimension';

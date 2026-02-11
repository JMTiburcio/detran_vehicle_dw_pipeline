-- Create view for change history (human-readable)
-- This view "explodes" the JSONB audit data for easier reading
--
-- STATUS: NOT READY - View defined but depends on core.audit_dim_veiculo which is not populated yet
-- Pipeline stage: Analytics (Phase 4)
-- TODO: This view will be usable once audit_dim_veiculo has data

CREATE OR REPLACE VIEW analytics.vw_historico_alteracoes AS
SELECT 
    a.id_audit,
    a.id_veiculo,
    dv.hash_veiculo,
    dv.codigo_fraga,
    a.operation,
    a.timestamp,
    a.user_name,
    -- Explode changed_fields JSONB
    jsonb_object_keys(a.changed_fields) AS campo_alterado,
    -- Get old and new values for each changed field
    a.old_values->>jsonb_object_keys(a.changed_fields) AS valor_antigo,
    a.new_values->>jsonb_object_keys(a.changed_fields) AS valor_novo
FROM core.audit_dim_veiculo a
INNER JOIN core.dim_veiculo dv ON a.id_veiculo = dv.id_veiculo
WHERE a.operation IN ('UPDATE', 'INSERT', 'DELETE')
ORDER BY a.timestamp DESC, a.id_audit DESC;

COMMENT ON VIEW analytics.vw_historico_alteracoes IS 'Human-readable view of audit trail - explodes JSONB for easier analysis';

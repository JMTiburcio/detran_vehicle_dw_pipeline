-- Create audit table for dim_veiculo_detran
-- Tracks changes using JSONB
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.audit_dim_veiculo_detran (
    id_audit SERIAL PRIMARY KEY,
    id_veiculo INTEGER NOT NULL REFERENCES core.dim_veiculo_detran(id_veiculo),
    operation VARCHAR(20) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    
    changed_fields JSONB,
    old_values JSONB,
    new_values JSONB,
    
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_name VARCHAR(255) DEFAULT CURRENT_USER
);

COMMENT ON TABLE core.audit_dim_veiculo_detran IS 'Audit trail for dim_veiculo_detran changes';

CREATE INDEX IF NOT EXISTS idx_audit_dim_veiculo_id ON core.audit_dim_veiculo_detran(id_veiculo);

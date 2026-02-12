-- Create audit table for tracking changes in dim_detran_veiculo
-- Stores historical changes using JSONB for flexibility
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.audit_dim_detran_veiculo (
    id_audit SERIAL PRIMARY KEY,
    id_veiculo INTEGER NOT NULL REFERENCES core.dim_detran_veiculo(id_veiculo),
    operation VARCHAR(20) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    
    -- JSONB for flexible schema (can handle dynamic columns)
    changed_fields JSONB,  -- Fields that changed: {"marca": true, "modelo": true}
    old_values JSONB,      -- Old values: {"marca": "FORD", "modelo": "FIESTA"}
    new_values JSONB,      -- New values: {"marca": "FORD", "modelo": "KA"}
    
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_name VARCHAR(255) DEFAULT CURRENT_USER
);

COMMENT ON TABLE core.audit_dim_detran_veiculo IS 'Audit trail for dim_detran_veiculo changes - stores history in JSONB format';
COMMENT ON COLUMN core.audit_dim_detran_veiculo.changed_fields IS 'JSONB object indicating which fields changed';
COMMENT ON COLUMN core.audit_dim_detran_veiculo.old_values IS 'JSONB object with old values before change';
COMMENT ON COLUMN core.audit_dim_detran_veiculo.new_values IS 'JSONB object with new values after change';

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_audit_detran_id_veiculo ON core.audit_dim_detran_veiculo(id_veiculo);

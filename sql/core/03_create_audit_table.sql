-- Create audit table for tracking changes
-- This table stores historical changes to dim_veiculo using JSONB for flexibility
--
-- STATUS: NOT READY - Structure defined but pipeline not implemented yet
-- Pipeline stage: Transform (Phase 3)
-- TODO: This table will be populated automatically by trigger when dim_veiculo is used

CREATE TABLE IF NOT EXISTS core.audit_dim_veiculo (
    id_audit SERIAL PRIMARY KEY,
    id_veiculo INTEGER NOT NULL REFERENCES core.dim_veiculo(id_veiculo),
    operation VARCHAR(20) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    
    -- JSONB for flexible schema (can handle dynamic columns)
    changed_fields JSONB,  -- Fields that changed: {"marca": true, "modelo": true}
    old_values JSONB,      -- Old values: {"marca": "FORD", "modelo": "KA"}
    new_values JSONB,      -- New values: {"marca": "FORD", "modelo": "KA 1.0"}
    
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_name VARCHAR(255) DEFAULT CURRENT_USER
);

COMMENT ON TABLE core.audit_dim_veiculo IS 'Audit trail for dim_veiculo changes - stores history in JSONB format';
COMMENT ON COLUMN core.audit_dim_veiculo.changed_fields IS 'JSONB object indicating which fields changed';
COMMENT ON COLUMN core.audit_dim_veiculo.old_values IS 'JSONB object with old values before change';
COMMENT ON COLUMN core.audit_dim_veiculo.new_values IS 'JSONB object with new values after change';

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_audit_id_veiculo ON core.audit_dim_veiculo(id_veiculo);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON core.audit_dim_veiculo(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_operation ON core.audit_dim_veiculo(operation);
CREATE INDEX IF NOT EXISTS idx_audit_changed_fields ON core.audit_dim_veiculo USING GIN(changed_fields);
CREATE INDEX IF NOT EXISTS idx_audit_old_values ON core.audit_dim_veiculo USING GIN(old_values);
CREATE INDEX IF NOT EXISTS idx_audit_new_values ON core.audit_dim_veiculo USING GIN(new_values);

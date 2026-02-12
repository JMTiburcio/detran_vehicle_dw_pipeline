-- Create audit table for fato_frota_uf
-- Tracks changes using JSONB
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.audit_fato_frota_uf (
    id_audit SERIAL PRIMARY KEY,
    id_fato INTEGER NOT NULL REFERENCES core.fato_frota_uf(id_fato),
    operation VARCHAR(20) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    
    changed_fields JSONB,
    old_values JSONB,
    new_values JSONB,
    
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_name VARCHAR(255) DEFAULT CURRENT_USER
);

COMMENT ON TABLE core.audit_fato_frota_uf IS 'Audit trail for fato_frota_uf changes';

CREATE INDEX IF NOT EXISTS idx_audit_fato_frota_id ON core.audit_fato_frota_uf(id_fato);

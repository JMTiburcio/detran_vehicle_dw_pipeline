-- Create trigger function to audit changes in fato_frota_uf
--
-- Pipeline stage: Transform (Phase 3)

CREATE OR REPLACE FUNCTION core.fn_audit_fato_frota_uf()
RETURNS TRIGGER AS $$
DECLARE
    changed_fields JSONB := '{}'::JSONB;
    old_values JSONB := '{}'::JSONB;
    new_values JSONB := '{}'::JSONB;
    col_name TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO core.audit_fato_frota_uf (
            id_fato, operation, changed_fields, old_values, new_values
        ) VALUES (
            NEW.id_fato,
            'INSERT',
            (SELECT jsonb_object_agg(key, true) FROM jsonb_each(to_jsonb(NEW) - 'id_fato' - 'created_at' - 'updated_at')),
            NULL,
            to_jsonb(NEW) - 'id_fato' - 'created_at' - 'updated_at'
        );
        RETURN NEW;
    END IF;
    
    IF TG_OP = 'UPDATE' THEN
        FOR col_name IN 
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'core' 
            AND table_name = 'fato_frota_uf'
            AND column_name NOT IN ('id_fato', 'created_at', 'updated_at')
        LOOP
            IF (to_jsonb(OLD)->>col_name) IS DISTINCT FROM (to_jsonb(NEW)->>col_name) THEN
                changed_fields := changed_fields || jsonb_build_object(col_name, true);
                old_values := old_values || jsonb_build_object(col_name, to_jsonb(OLD)->>col_name);
                new_values := new_values || jsonb_build_object(col_name, to_jsonb(NEW)->>col_name);
            END IF;
        END LOOP;
        
        IF changed_fields != '{}'::JSONB THEN
            INSERT INTO core.audit_fato_frota_uf (
                id_fato, operation, changed_fields, old_values, new_values
            ) VALUES (
                NEW.id_fato, 'UPDATE', changed_fields, old_values, new_values
            );
        END IF;
        
        NEW.updated_at := CURRENT_TIMESTAMP;
        RETURN NEW;
    END IF;
    
    IF TG_OP = 'DELETE' THEN
        INSERT INTO core.audit_fato_frota_uf (
            id_fato, operation, changed_fields, old_values, new_values
        ) VALUES (
            OLD.id_fato,
            'DELETE',
            (SELECT jsonb_object_agg(key, true) FROM jsonb_each(to_jsonb(OLD) - 'id_fato' - 'created_at' - 'updated_at')),
            to_jsonb(OLD) - 'id_fato' - 'created_at' - 'updated_at',
            NULL
        );
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_fato_frota_uf ON core.fato_frota_uf;

CREATE TRIGGER trg_audit_fato_frota_uf
    AFTER INSERT OR UPDATE OR DELETE ON core.fato_frota_uf
    FOR EACH ROW
    EXECUTE FUNCTION core.fn_audit_fato_frota_uf();

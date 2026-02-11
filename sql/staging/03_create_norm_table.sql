-- Create normalized table
-- This table stores normalized/standardized data ready for DW
--
-- STATUS: READY - Structure defined for Phase 2 normalization
-- Pipeline stage: Normalize (Phase 2)
--
-- Structure:
-- - All columns from raw table (translated to English)
-- - Original columns (English names) + Normalized columns (English names with _norm suffix)
-- - Records are exploded by year (one row per year in the range)

CREATE TABLE IF NOT EXISTS staging.fraga_vehicle_norm (
    id_norm SERIAL PRIMARY KEY,
    
    -- Original columns from raw table (translated to English)
    fraga_code VARCHAR(255),  -- codigo_fraga
    brand VARCHAR(255),  -- marca (original)
    model VARCHAR(255),  -- modelo_veiculo (original)
    version VARCHAR(255),  -- nome_veiculo (original)
    engine_name VARCHAR(255),  -- nome_motor (original)
    engine_config VARCHAR(255),  -- configuracao_motor (original)
    power_cv VARCHAR(255),  -- potencia_cv
    fuel VARCHAR(255),  -- combustivel (original)
    aspiration VARCHAR(255),  -- aspiracao (original)
    ktype_tecdoc VARCHAR(255),  -- ktype_tecdoc
    classification VARCHAR(255),  -- classificacao
    vehicle_type VARCHAR(255),  -- tipo_veiculo
    vehicle_category VARCHAR(255),  -- categoria_veiculo
    generation VARCHAR(255),  -- geracao
    displacement_liters VARCHAR(255),  -- cilindrada_litros
    command VARCHAR(255),  -- comando
    cylinder_count VARCHAR(255),  -- numero_cilindros
    transmission VARCHAR(255),  -- tipo_transmissao (original)
    cabin_type VARCHAR(255),  -- tipo_cabine
    fipe_code VARCHAR(255),  -- codigo_fipe
    
    -- Normalized columns (English - normalized text)
    brand_norm VARCHAR(255),  -- marca normalized
    model_norm VARCHAR(255),  -- modelo_veiculo normalized
    version_norm VARCHAR(255),  -- nome_veiculo normalized
    engine_name_norm VARCHAR(255),  -- nome_motor normalized
    engine_config_norm VARCHAR(255),  -- configuracao_motor normalized
    fuel_norm VARCHAR(255),  -- combustivel normalized
    transmission_norm VARCHAR(255),  -- tipo_transmissao normalized
    aspiration_norm VARCHAR(255),  -- aspiracao normalized
    
    -- Year (from explosion of date range)
    year INTEGER,
    
    -- Metadata columns (from raw table)
    load_timestamp TIMESTAMP,
    source_file VARCHAR(500),
    excel_row INTEGER,
    
    -- Additional metadata
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_raw INTEGER REFERENCES staging.fraga_vehicle_raw(id_raw)
);

COMMENT ON TABLE staging.fraga_vehicle_norm IS 'Normalized vehicle data ready for DW layer. Records exploded by year. Loaded by INSERT only after truncate.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_norm_fraga_code ON staging.fraga_vehicle_norm(fraga_code);
CREATE INDEX IF NOT EXISTS idx_norm_id_raw ON staging.fraga_vehicle_norm(id_raw);
CREATE INDEX IF NOT EXISTS idx_norm_year ON staging.fraga_vehicle_norm(year);

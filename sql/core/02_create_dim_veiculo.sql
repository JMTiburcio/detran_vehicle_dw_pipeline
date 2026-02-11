-- Create dimension table for vehicles
-- Same attributes as staging.fraga_vehicle_norm (original + normalized) plus hash_veiculo for unique identification.
-- SCD Type 1: Current state in table; history in core.audit_dim_veiculo (via trigger).
--
-- Pipeline stage: Transform (Phase 3)

CREATE TABLE IF NOT EXISTS core.dim_veiculo (
    id_veiculo SERIAL PRIMARY KEY,
    hash_veiculo VARCHAR(64) NOT NULL UNIQUE,

    -- Original columns (from norm, English names)
    fraga_code VARCHAR(255),
    brand VARCHAR(255),
    model VARCHAR(255),
    version VARCHAR(255),
    engine_name VARCHAR(255),
    engine_config VARCHAR(255),
    power_cv VARCHAR(255),
    fuel VARCHAR(255),
    aspiration VARCHAR(255),
    ktype_tecdoc VARCHAR(255),
    classification VARCHAR(255),
    vehicle_type VARCHAR(255),
    vehicle_category VARCHAR(255),
    generation VARCHAR(255),
    displacement_liters VARCHAR(255),
    command VARCHAR(255),
    cylinder_count VARCHAR(255),
    transmission VARCHAR(255),
    cabin_type VARCHAR(255),
    fipe_code VARCHAR(255),

    -- Normalized columns (from norm)
    brand_norm VARCHAR(255),
    model_norm VARCHAR(255),
    version_norm VARCHAR(255),
    engine_name_norm VARCHAR(255),
    engine_config_norm VARCHAR(255),
    fuel_norm VARCHAR(255),
    transmission_norm VARCHAR(255),
    aspiration_norm VARCHAR(255),

    -- Year (from norm explosion)
    year INTEGER,

    -- Metadata from source (norm)
    load_timestamp TIMESTAMP,
    source_file VARCHAR(500),
    excel_row INTEGER,

    -- Core metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE core.dim_veiculo IS 'Dimension table for vehicles - same grain as norm, with hash_veiculo for unique identification. History in audit_dim_veiculo.';
COMMENT ON COLUMN core.dim_veiculo.id_veiculo IS 'Surrogate key (technical PK)';
COMMENT ON COLUMN core.dim_veiculo.hash_veiculo IS 'Natural business key: SHA256 of brand_norm|model_norm|version_norm|fuel_norm|transmission_norm|aspiration_norm|engine_config_norm|engine_name_norm|fraga_code|year';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dim_hash_veiculo ON core.dim_veiculo(hash_veiculo);
CREATE INDEX IF NOT EXISTS idx_dim_fraga_code ON core.dim_veiculo(fraga_code);
CREATE INDEX IF NOT EXISTS idx_dim_year ON core.dim_veiculo(year);
CREATE INDEX IF NOT EXISTS idx_dim_brand_model ON core.dim_veiculo(brand_norm, model_norm);

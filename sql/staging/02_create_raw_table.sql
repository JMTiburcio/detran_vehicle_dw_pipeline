-- Create raw table for csv data
-- This table stores data exactly as it comes from csv
-- Structure based on analysis of frota_detran_por_uf_2025.csv (updated: removed unused columns)
--
-- STATUS: READY - This table is ready for use
-- Pipeline stage: Extract + Load Raw (Phase 1)

UF;Marca Modelo;Ano Fabricação Veículo CRV;Qtd. Veículos

CREATE TABLE IF NOT EXISTS staging.detran_vehicle_raw (
    id_raw SERIAL PRIMARY KEY,
    -- Original columns from detran csv (normalized names, Portuguese)
    uf VARCHAR(100),  -- Original: UF
    marca_modelo VARCHAR(100),  -- Original: Marca Modelo
    ano_fabricacao_veiculo_crv VARCHAR(100),  -- Original: Ano Fabricação Veículo CRV
    qtd_veiculos VARCHAR(100),  -- Original: Qtd. Veículos
    -- Metadata columns (English)
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    csv_row INTEGER
);

COMMENT ON TABLE staging.detran_vehicle_raw IS 'Raw data from detran csv files, stored exactly as received (Total rows analyzed: 17,167)';

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_raw_codigo_detran ON staging.detran_vehicle_raw(codigo_detran);
CREATE INDEX IF NOT EXISTS idx_raw_load_timestamp ON staging.detran_vehicle_raw(load_timestamp);

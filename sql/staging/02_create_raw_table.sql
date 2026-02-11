-- Create raw table for Excel data
-- This table stores data exactly as it comes from Excel
-- Structure based on analysis of fraga_veiculos_v2.xlsx (updated: removed unused columns)
--
-- STATUS: READY - This table is ready for use
-- Pipeline stage: Extract + Load Raw (Phase 1)

CREATE TABLE IF NOT EXISTS staging.fraga_vehicle_raw (
    id_raw SERIAL PRIMARY KEY,
    -- Original columns from Fraga Excel (normalized names, Portuguese)
    codigo_fraga VARCHAR(100),  -- Original: Código Fraga
    marca VARCHAR(100),  -- Original: Marca
    nome_veiculo VARCHAR(100),  -- Original: Nome Veículo
    modelo_veiculo VARCHAR(100),  -- Original: Modelo Veículo
    nome_motor VARCHAR(100),  -- Original: Nome Motor
    configuracao_motor VARCHAR(100),  -- Original: Configuração Motor
    potencia_cv VARCHAR(100),  -- Original: Potência CV
    combustivel VARCHAR(100),  -- Original: Combustivel
    aspiracao VARCHAR(100),  -- Original: Aspiração
    ktype_tecdoc VARCHAR(255),  -- Original: KType Tecdoc
    classificacao VARCHAR(100),  -- Original: Classificação
    tipo_veiculo VARCHAR(100),  -- Original: Tipo Veículo
    categoria_veiculo VARCHAR(100),  -- Original: Categoria Veículo
    geracao VARCHAR(100),  -- Original: Geração
    cilindrada_litros VARCHAR(100),  -- Original: Cilindrada Litros (stored as text, converted in normalized)
    comando VARCHAR(100),  -- Original: Comando
    numero_cilindros VARCHAR(100),  -- Original: Número Cilindros (stored as text, converted in normalized)
    tipo_transmissao VARCHAR(100),  -- Original: Tipo Transmissão
    data_inicio VARCHAR(100),  -- Original: Data Início (stored as text, converted in normalized)
    data_final VARCHAR(100),  -- Original: Data Final (stored as text, converted in normalized)
    tipo_cabine VARCHAR(255),  -- Original: Tipo Cabine
    codigo_fipe VARCHAR(100),  -- Original: Código Fipe
    -- Metadata columns (English)
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    excel_row INTEGER
);

COMMENT ON TABLE staging.fraga_vehicle_raw IS 'Raw data from Fraga Excel files, stored exactly as received (Total rows analyzed: 17,167)';

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_raw_codigo_fraga ON staging.fraga_vehicle_raw(codigo_fraga);
CREATE INDEX IF NOT EXISTS idx_raw_load_timestamp ON staging.fraga_vehicle_raw(load_timestamp);

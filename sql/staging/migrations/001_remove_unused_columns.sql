-- Migration: Remove unused columns from staging.fraga_vehicle_raw
-- Date: 2024
-- Reason: Columns removed from Excel source file
--
-- Removed columns:
-- - modelo_motor
-- - ignicao
-- - modelo_transmissao
-- - numero_portas

-- Remove columns if they exist (safe to run multiple times)
ALTER TABLE staging.fraga_vehicle_raw 
DROP COLUMN IF EXISTS modelo_motor,
DROP COLUMN IF EXISTS ignicao,
DROP COLUMN IF EXISTS modelo_transmissao,
DROP COLUMN IF EXISTS numero_portas;

COMMENT ON TABLE staging.fraga_vehicle_raw IS 'Raw data from Fraga Excel files - Updated: removed unused columns (modelo_motor, ignicao, modelo_transmissao, numero_portas)';

-- Create staging schema
-- This schema holds raw and intermediate data

CREATE SCHEMA IF NOT EXISTS staging;

COMMENT ON SCHEMA staging IS 'Staging layer for raw and intermediate data from source systems';

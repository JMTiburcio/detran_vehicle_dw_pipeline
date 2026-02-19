BEGIN;

-- ==============================
-- ROLLBACK dim_veiculo_detran
-- ==============================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
          AND table_name = 'dim_veiculo_detran_prev'
    ) THEN

        ALTER TABLE analytics.dim_veiculo_detran
        RENAME TO dim_veiculo_detran_failed;

        ALTER TABLE analytics.dim_veiculo_detran_prev
        RENAME TO dim_veiculo_detran;

        -- DROP TABLE analytics.dim_veiculo_detran_failed;

    ELSE
        RAISE NOTICE 'Tabela dim_veiculo_detran_prev não encontrada. Rollback ignorado.';
    END IF;
END $$;


-- ==============================
-- ROLLBACK fato_frota_uf
-- ==============================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
          AND table_name = 'fato_frota_uf_prev'
    ) THEN

        ALTER TABLE analytics.fato_frota_uf
        RENAME TO fato_frota_uf_failed;

        ALTER TABLE analytics.fato_frota_uf_prev
        RENAME TO fato_frota_uf;

        -- DROP TABLE analytics.fato_frota_uf_failed;

    ELSE
        RAISE NOTICE 'Tabela fato_frota_uf_prev não encontrada. Rollback ignorado.';
    END IF;
END $$;

COMMIT;

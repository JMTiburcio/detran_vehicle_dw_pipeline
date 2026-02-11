# Guia de Execução Manual

## Pré-requisitos

1. PostgreSQL instalado e rodando
2. Python 3.8+ instalado
3. Dependências instaladas: `pip install -r requirements.txt`
4. Arquivo `.env` configurado com credenciais do banco

## Setup Inicial

### 1. Criar Schemas e Tabelas

Execute os scripts SQL na ordem:

```bash
# Staging
psql -U postgres -d fraga_dw -f sql/staging/01_create_schema.sql
psql -U postgres -d fraga_dw -f sql/staging/02_create_raw_table.sql
psql -U postgres -d fraga_dw -f sql/staging/03_create_norm_table.sql

# Core
psql -U postgres -d fraga_dw -f sql/core/01_create_schema.sql
psql -U postgres -d fraga_dw -f sql/core/02_create_dim_veiculo.sql
psql -U postgres -d fraga_dw -f sql/core/03_create_audit_table.sql
psql -U postgres -d fraga_dw -f sql/core/04_create_trigger.sql

# Analytics
psql -U postgres -d fraga_dw -f sql/analytics/01_create_schema.sql
psql -U postgres -d fraga_dw -f sql/analytics/02_create_vw_dim_veiculo.sql
psql -U postgres -d fraga_dw -f sql/analytics/03_create_vw_historico_alteracoes.sql
```

### 2. Colocar Arquivo Excel

Coloque o arquivo Excel da Fraga em `data/input/`

## Execução do Pipeline

### Execução Completa

```bash
python orchestration/run_pipeline.py
```

### Execução por Etapas

TODO: Documentar como executar cada etapa individualmente

## Checklist de Execução

- [ ] Schemas e tabelas criados
- [ ] Arquivo Excel em `data/input/`
- [ ] `.env` configurado
- [ ] Pipeline executado
- [ ] Validações passaram
- [ ] Dados visíveis em `analytics.vw_dim_veiculo`

## Troubleshooting

TODO: Adicionar problemas comuns e soluções

## Reprocessamento

Para reprocessar dados:

1. Truncar tabelas de staging (opcional)
2. Executar pipeline novamente
3. Upsert garantirá que não haja duplicatas

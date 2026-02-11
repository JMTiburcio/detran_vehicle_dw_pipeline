# Arquitetura do Projeto

## Visão Geral

Este projeto implementa um Data Warehouse em Postgres para migrar dados de veículos da Fraga (Excel) para um ambiente estruturado e confiável.

## Decisões Arquiteturais

### 3 Camadas Lógicas

1. **Staging** - Dados crus e intermediários
2. **DW** - Dados confiáveis, modelados e históricos
3. **Analytics** - Views/tabelas prontas para BI

### Modelagem

- **Dimensão Veículo**: Tratada como dimensão central
- **SCD Tipo 1**: Sem histórico na dimensão (histórico na tabela de auditoria)
- **Chave Natural**: `hash_veiculo` baseado em atributos normalizados
- **Chave Técnica**: `id_veiculo` (SERIAL PRIMARY KEY)

### Rastreabilidade

- **Trigger no Postgres**: Captura automaticamente todas as mudanças
- **Tabela de Auditoria**: `core.audit_dim_veiculo` com JSONB para flexibilidade
- **View de Histórico**: `analytics.vw_historico_alteracoes` para leitura humana

## Fluxo de Dados

```
Excel (Fraga)
   ↓
staging.fraga_vehicle_raw
   ↓
Normalização (Python)
   ↓
staging.fraga_vehicle_norm
   ↓
Upsert (Python)
   ↓
core.dim_veiculo (Trigger → audit_dim_veiculo)
   ↓
analytics.vw_dim_veiculo
   ↓
BI
```

## Tecnologias

- **Python 3.8+**: Linguagem principal
- **PostgreSQL**: Banco de dados
- **pandas**: Manipulação de dados
- **psycopg2**: Conexão com Postgres
- **openpyxl**: Leitura de Excel

## Decisões Conscientes (Não Usar Agora)

- ❌ dbt
- ❌ Airflow / orquestrador pesado
- ❌ Múltiplas fontes no piloto

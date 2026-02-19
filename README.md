# Fraga Vehicle DW Pipeline

Pipeline de Data Warehouse para migração de dados de veículos da Fraga (Excel) para Postgres.

## Arquitetura

O pipeline segue uma arquitetura de 3 camadas:

```
Excel (Fraga) → staging → core → analytics → BI
```

### Schemas Postgres

- `staging` - Dados crus e intermediários
- `core` - Dados confiáveis, modelados e históricos
- `analytics` - Réplicas estáveis do core para BI (atualizadas por swap atômico); consumir `analytics.dim_veiculo_detran` e `analytics.fato_frota_uf` diretamente. Ver [docs/analytics_bi.md](docs/analytics_bi.md).

## Estrutura do Projeto

```
fraga_vehicle_dw_pipeline/
├── config/          # Configurações e parâmetros
├── data/            # Arquivos de entrada, processados e logs
├── pipeline/        # Módulos do pipeline (extract, load, normalize, transform, validate)
├── sql/             # Scripts SQL organizados por schema
├── orchestration/   # Ponto único de execução
├── tests/           # Testes de qualidade
└── docs/            # Documentação e regras
```

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

1. Copie `.env.example` para `.env`
2. Configure as variáveis de conexão com Postgres

## Execução

```bash
python orchestration/run_pipeline.py
```

## Fases do Projeto

- **Fase 1**: Fundamentos (schemas, tabelas, pipeline base)
- **Fase 2**: Normalização e chave de negócio (hash_veiculo, upsert)
- **Fase 3**: Histórico e rastreabilidade (triggers, audit)
- **Fase 4**: Governança mínima (documentação, logs, validações)

## Status

🚧 Em desenvolvimento - Fase de esqueleto inicial

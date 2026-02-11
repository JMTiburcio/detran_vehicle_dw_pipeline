# Scripts de Análise

Esta pasta contém scripts temporários usados para análise e definição da estrutura do projeto.

## Arquivos

- `analyze_excel.py` - Script usado para analisar o arquivo Excel `fraga_veiculos_v2.xlsx` e gerar sugestão de estrutura da tabela `staging.fraga_vehicle_raw`
- `analyze_raw_data.py` - Script para analisar dados reais da tabela `staging.fraga_vehicle_raw` e gerar estatísticas para decisões de normalização
- `sql_suggestion_raw_table.sql` - Sugestão de CREATE TABLE gerada pela análise do Excel

## Uso

### Análise do Excel (antes de carregar)

```bash
python scripts/analysis/analyze_excel.py
```

Gera:
- Relatório detalhado da estrutura do Excel
- Sugestão de CREATE TABLE SQL
- Estatísticas de dados (nulos, únicos, tipos)

### Análise dos Dados Raw (após carregar)

```bash
python scripts/analysis/analyze_raw_data.py
```

Gera:
- Variações de marcas, combustíveis, transmissões, aspiração
- Distribuição de valores NULL
- Análise de anos (data_inicio, data_final)
- Duplicatas potenciais
- Sugestões de mapeamento RAW → NORM
- Análise de campos numéricos para conversão

## Data da Análise

Análise inicial realizada em: 2024
- Arquivo analisado: `data/input/fraga_veiculos_v2.xlsx`
- Total de linhas: 17,167
- Total de colunas: 26

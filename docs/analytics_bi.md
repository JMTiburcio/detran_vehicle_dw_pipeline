# Analytics – consumo pelo BI

O schema **analytics** contém réplicas estáveis das tabelas do core para consumo pelo BI. As tabelas são atualizadas por **swap atômico** ao final do pipeline (Phase 4), evitando truncate in-place e janelas de inconsistência.

## Tabelas para o BI

Consuma **diretamente** as tabelas abaixo (sem VIEWs):

| Tabela | Descrição |
|--------|-----------|
| `analytics.dim_veiculo_detran` | Dimensão de veículos (marca, modelo, ano_fabricacao, descricao_detran). Mesma estrutura de `core.dim_veiculo_detran`. |
| `analytics.fato_frota_uf` | Fatos de frota por UF e por veículo. Uma única tabela com todos os períodos; use a coluna `report_period` (YYYYMM) para filtrar. Mesma estrutura de `core.fato_frota_uf`. |

## Estrutura

- **dim_veiculo_detran**: `id_veiculo`, `hash_veiculo`, `marca`, `modelo`, `ano_fabricacao`, `descricao_detran`, `created_at`, `updated_at`.
- **fato_frota_uf**: `report_period`, `id_fato`, `id_veiculo`, `uf`, `frota`, `id_raw`, `created_at`, `updated_at`. PK `(report_period, id_fato)`; único por `(report_period, id_veiculo, uf)`.

## Atualização

- A cada execução completa do pipeline (até a phase analytics), os dados do core são copiados para tabelas temporárias e em seguida é feita a troca atômica: a versão atual passa a ser `*_prev` e a nova passa a ser a tabela final.
- São mantidas apenas a **versão atual** (nome final) e a **versão anterior** (`analytics.dim_veiculo_detran_prev`, `analytics.fato_frota_uf_prev`). Não há backups com timestamp.

## Uso no BI

- Apontar conexões/consultas para `analytics.dim_veiculo_detran` e `analytics.fato_frota_uf`.
- Para análises por período, filtrar `analytics.fato_frota_uf` por `report_period` (ex.: `WHERE report_period = 202501`).
- Join: `fato_frota_uf.id_veiculo` → `dim_veiculo_detran.id_veiculo`.

# Migrações de Schema - Staging

Esta pasta contém scripts de migração para atualizar a estrutura das tabelas do schema `staging`.

## Como usar

Execute as migrações na ordem numérica:

```bash
psql -U postgres -d seu_banco -f sql/staging/migrations/001_remove_unused_columns.sql
```

Ou usando o script Python:

```python
from pipeline.utils import get_db_connection_from_env, execute_sql_file
conn = get_db_connection_from_env()
execute_sql_file('sql/staging/migrations/001_remove_unused_columns.sql', conn)
conn.close()
```

## Migrações

### 001_remove_unused_columns.sql
- **Data**: 2024
- **Descrição**: Remove colunas não utilizadas do Excel
- **Colunas removidas**: modelo_motor, ignicao, modelo_transmissao, numero_portas
- **Seguro**: Pode ser executado múltiplas vezes (usa IF EXISTS)

## Nota

Após executar migrações, atualize o arquivo `sql/staging/02_create_raw_table.sql` para refletir a nova estrutura, assim futuras criações da tabela já terão a estrutura correta.

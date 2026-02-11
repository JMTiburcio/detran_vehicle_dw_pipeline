# Recomendações de Normalização - Baseadas em Análise Real

**Data da análise**: 04/02/2026
**Total de registros analisados**: 17,167  
**Fonte**: `staging.fraga_vehicle_raw`

## Decisões Recomendadas

### 1. Normalização de Texto

#### ✅ SIM - Remover acentos
**Justificativa**: Facilita comparações
- Exemplo: "ELÉTRICO" → "ELETRICO"

#### ✅ SIM - Uppercase
**Justificativa**: Consistência e facilita comparações
- Exemplo: "gasolina" → "GASOLINA"

#### ✅ SIM - Normalizar espaços
**Justificativa**: Remove inconsistências
- Exemplo: "MERCEDES  BENZ" → "MERCEDES BENZ"
- Trim espaços no início/fim
- Reduzir múltiplos espaços para um único

#### ⚠️ PARCIAL - Caracteres especiais
**Justificativa**: Manter alguns, remover outros
- **Manter**: Hífen (ex: "LAND ROVER", "HARLEY-DAVIDSON")
- **Remover**: Pontos, vírgulas, ponto-e-vírgula (ex: "AUTOMÁTICA; MANUAL" → "AUTOMATICA MANUAL")
- **Tratar**: Ponto-e-vírgula como separador de múltiplos valores → escolher primeiro ou concatenar

### 2. Mapeamento de Colunas RAW → NORM

**Confirmado baseado na análise:**

| RAW | NORM | Observação |
|-----|------|------------|
| `marca` | `marca` | Direto |
| `modelo_veiculo` | `modelo` | Direto (ex: "GS", "3.5 V6") |
| `nome_veiculo` | `versao` | Direto (ex: "INTEGRA", "RDX", "LEGEND") |
| `nome_motor` | `motor` | Direto (ex: "B18", "J35Z2") |
| `tipo_transmissao` | `transmissao` | Direto (precisa normalização forte) |
| `combustivel` | `combustivel` | Direto (precisa normalização) |
| `aspiracao` | `aspiracao` | Direto (já está limpo) |

**Colunas não mapeadas (não irão para NORM):**
- `configuracao_motor`, `potencia_cv`, `ktype_tecdoc`
- `classificacao`, `tipo_veiculo`, `categoria_veiculo`, `geracao`
- `cilindrada_litros`, `comando`, `numero_cilindros`
- `data_inicio`, `data_final`, `tipo_cabine`, `codigo_fipe`

### 3. Dicionários de Normalização

#### 3.1 Marcas (292 valores únicos)
**Recomendação**: Criar dicionário baseado nos dados reais

**Variações identificadas que precisam mapeamento:**
- "MERCEDES BENZ" (já está padronizado)
- Verificar se há "VW" → "VOLKSWAGEN" (não apareceu na análise, mas pode existir)
- Verificar variações com/sem espaços, hífens

**Estratégia**: 
1. Normalizar texto primeiro (uppercase, sem acentos)
2. Depois aplicar dicionário
3. Se não encontrar no dicionário, usar valor normalizado

#### 3.2 Combustíveis (11 valores únicos)
**Valores encontrados:**
- GASOLINA (40.1%)
- DIESEL (33.4%)
- FLEX (17.6%)
- ETANOL (4.0%)
- ELÉTRICO (2.9%) → normalizar para "ELETRICO"
- GASOLINA/ELÉTRICO (1.6%) → decidir: "GASOLINA ELETRICO" ou "HIBRIDO"?
- DIESEL/ELÉTRICO (0.2%)
- GAS VEICULAR (0.1%)
- FLEX/ELÉTRICO (0.1%)
- FLEX/GNV (0.0%)
- "-" (0.0%) → NULL

**Recomendação de dicionário:**
```python
NORMALIZE_COMBUSTIVEIS = {
    "GASOLINA": "GASOLINA",
    "DIESEL": "DIESEL",
    "FLEX": "FLEX",
    "ETANOL": "ETANOL",
    "ELETRICO": "ELETRICO",  # sem acento
    "ELÉTRICO": "ELETRICO",  # com acento
    "GASOLINA/ELETRICO": "HIBRIDO GASOLINA",
    "GASOLINA/ELÉTRICO": "HIBRIDO GASOLINA",
    "DIESEL/ELETRICO": "HIBRIDO DIESEL",
    "DIESEL/ELÉTRICO": "HIBRIDO DIESEL",
    "FLEX/ELETRICO": "HIBRIDO FLEX",
    "FLEX/ELÉTRICO": "HIBRIDO FLEX",
    "GAS VEICULAR": "GNV",
    "FLEX/GNV": "FLEX GNV",
    "-": None  # NULL
}
```

#### 3.3 Transmissões (47 valores únicos - PROBLEMA!)
**Situação crítica**: Muitas variações e valores compostos

**Valores principais:**
- MANUAL (52.1%)
- AUTOMÁTICA (23.0%) → "AUTOMATICA"
- AUTOMATIZADA (10.2%)
- "-" (7.5%) → NULL
- CVT (2.8%)
- Valores compostos com ";" (ex: "AUTOMATIZADA; MANUAL")

**Recomendação de dicionário:**
```python
NORMALIZE_TRANSMISSOES = {
    "MANUAL": "MANUAL",
    "AUTOMATICA": "AUTOMATICA",
    "AUTOMÁTICA": "AUTOMATICA",
    "AUTOMATIZADA": "AUTOMATIZADA",
    "CVT": "CVT",
    "MECANICA": "MANUAL",  # Mecânica = Manual
    "MECÂNICA": "MANUAL",
    "SINCRONIZADA": "MANUAL",
    "SEMIAUTOMATICA": "SEMIAUTOMATICA",
    "SEMI-AUTOMATIZADA": "SEMIAUTOMATICA",
    # Valores compostos: pegar primeiro
    "AUTOMATIZADA; MANUAL": "AUTOMATIZADA",
    "AUTOMATICA; MANUAL": "AUTOMATICA",
    # Outros valores específicos manter como estão após normalização
    "-": None  # NULL
}
```

**Estratégia para valores compostos:**
- Se contém ";", pegar primeiro valor antes do ";"
- Normalizar o valor escolhido

#### 3.4 Aspiração (5 valores únicos)
**Valores encontrados:**
- ASPIRADO (57.2%)
- TURBO (38.3%)
- BITURBO (0.8%)
- COMPRESSOR (0.7%)
- "-" (0.1%) → NULL

**Recomendação**: Não precisa de dicionário complexo, apenas normalização de texto
- Já está bem padronizado
- Apenas tratar "-" como NULL

#### Resultado esperado:
- **Unicidade atual**: 86.8% (14,902 únicos de 17,167)
- **Duplicatas**: 1,660 combinações duplicadas (3,925 registros)
- **Ação**: Manter duplicatas (diferentes id_raw)

### 5. Range de Anos

#### ❌ NÃO explodir range
**Justificativa**:
- 78.5% têm range > 1 ano (média de 3.9 anos)
- Explodir criaria muitas linhas (ex: 1990-1995 = 6 linhas)
- Range médio de 3.9 anos = ~4x mais registros

**Recomendação**: 
- Manter `data_inicio` e `data_final` como colunas separadas na NORM
- Converter para INTEGER na normalização
- Tratar NULL em data_final como "ainda em produção" ou NULL

#### Tratamento de anos faltantes:
- **data_inicio NULL**: Não ocorre (100% preenchido)
- **data_final NULL**: 24.9% (4,266 registros)
  - **Recomendação**: Deixar como NULL (indica "ainda em produção" ou desconhecido)

### 6. Conversão de Tipos

#### ✅ Converter na NORM
**Justificativa**: RAW armazena como texto, NORM deve ter tipos corretos

#### Campos a converter:

| Campo RAW | Tipo NORM | Observação |
|-----------|-----------|------------|
| `cilindrada_litros` | NUMERIC(5,2) | Tratar vírgula como ponto (ex: "3,5" → 3.5) |
| `numero_cilindros` | INTEGER | Apenas "-" inválido (0.2%) |
| `potencia_cv` | INTEGER | Muitos com vírgula (22% inválidos) - tratar |
| `data_inicio` | INTEGER | Já está OK (100% válido) |
| `data_final` | INTEGER | Já está OK (100% dos preenchidos) |

#### Tratamento de valores inválidos:
- **"-"** → NULL
- **Vírgula como decimal**: "3,5" → 3.5 (formato brasileiro)
- **Valores com ponto e vírgula**: Pegar primeiro valor ou NULL
- **Strings não numéricas**: NULL

### 7. Códigos Fraga

#### ❌ NÃO criar tabela separada (por enquanto)
**Justificativa**:
- Análise mostra: Nenhum codigo_fraga duplicado
- Cada registro RAW tem código único
- 1:1 entre RAW e código Fraga

**Recomendação**: 
- Manter `codigo_fraga` na tabela NORM

### 8. Validações

#### Campos obrigatórios:
- ✅ `marca` (0% NULL)
- ✅ `modelo` (via modelo_veiculo, 0% NULL)

#### Validações adicionais:
- `data_inicio` deve ser <= `data_final` (se ambos preenchidos)
- `data_inicio` deve ser >= 1900 e <= 2100 (validação de range razoável)

### 9. Tratamento de Duplicatas

#### ✅ MANTER TODAS as duplicatas
**Justificativa**:
- 86.8% de unicidade é bom
- Duplicatas podem ser:
  - Mesmo veículo em diferentes cargas (histórico)
  - Variações legítimas (ex: mesmo modelo, anos diferentes)
- Manter `id_raw` permite rastreabilidade

**Estratégia**:
- Na fase de Transform (Phase 3), fazer UPSERT baseado em hash

### 10. Estrutura da Tabela NORM

**Confirmar se precisa ajustar** `sql/staging/03_create_norm_table.sql`:

**Colunas essenciais (confirmadas):**
- hash_veiculo, codigo_fraga
- marca, modelo, versao, motor, combustivel, transmissao, aspiracao
- marca_norm, modelo_norm, versao_norm, motor_norm, combustivel_norm, transmissao_norm, aspiracao_norm

**Questão**: Precisamos de mais colunas na NORM?
- Anos? (data_inicio, data_final) → **SIM, recomendado**
- Outras colunas da RAW? → Decidir baseado em necessidade do DW

## Resumo das Decisões Críticas

1. ✅ **Normalização de texto**: Uppercase + remover acentos + normalizar espaços
2. ✅ **Hash**: SHA256, ordem fixa, NULL = ""
3. ✅ **Anos**: NÃO explodir, manter range (data_inicio, data_final)
4. ✅ **Duplicatas**: Manter todas (histórico)
5. ✅ **Códigos Fraga**: Manter na NORM (1:1 por enquanto)
6. ✅ **Conversão de tipos**: SIM, na NORM, tratar vírgula como decimal
7. ⚠️ **Dicionários**: Criar para combustíveis e transmissões (marcas pode ser incremental)
8. ⚠️ **Valores compostos**: Transmissões com ";" → pegar primeiro valor

## Próximos Passos

1. ✅ Análise concluída - dados suficientes
2. ⏭️ Implementar funções de normalização baseadas nestas decisões
3. ⏭️ Criar dicionários iniciais (pode expandir depois)
4. ⏭️ Implementar geração de hash
5. ⏭️ Implementar carregamento na NORM

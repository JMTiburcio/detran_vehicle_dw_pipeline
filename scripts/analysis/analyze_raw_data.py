"""
Script para analisar dados reais da tabela staging.fraga_vehicle_raw.

Este script analisa os dados carregados na tabela raw para:
- Identificar variações de marcas, combustíveis, transmissões, aspiração
- Verificar distribuição de valores NULL
- Analisar padrões de anos (data_inicio, data_final)
- Identificar duplicatas potenciais
- Sugerir mapeamento de colunas RAW → NORM
- Gerar estatísticas para decisões de normalização

Uso:
    python scripts/analysis/analyze_raw_data.py
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipeline.utils import get_db_connection_from_env

load_dotenv()


def analyze_raw_data():
    """Analisa dados da tabela staging.fraga_vehicle_raw."""
    print("=" * 80)
    print("ANALISE DE DADOS - staging.fraga_vehicle_raw")
    print("=" * 80)
    print()
    
    try:
        # Conectar ao banco
        print("Conectando ao banco de dados...")
        conn = get_db_connection_from_env()
        
        # Ler dados
        print("Lendo dados da tabela staging.fraga_vehicle_raw...")
        query = "SELECT * FROM staging.fraga_vehicle_raw"
        df = pd.read_sql(query, conn)
        
        print(f"Total de registros: {len(df):,}")
        print(f"Total de colunas: {len(df.columns)}")
        print()
        
        conn.close()
        
        # Análises
        print("=" * 80)
        print("1. DISTRIBUICAO DE VALORES NULL")
        print("=" * 80)
        analyze_nulls(df)
        
        print()
        print("=" * 80)
        print("2. VARIACOES DE MARCAS")
        print("=" * 80)
        analyze_marcas(df)
        
        print()
        print("=" * 80)
        print("3. VARIACOES DE COMBUSTIVEIS")
        print("=" * 80)
        analyze_combustiveis(df)
        
        print()
        print("=" * 80)
        print("4. VARIACOES DE TRANSMISSOES")
        print("=" * 80)
        analyze_transmissoes(df)
        
        print()
        print("=" * 80)
        print("5. VARIACOES DE ASPIRACAO")
        print("=" * 80)
        analyze_aspiracao(df)
        
        print()
        print("=" * 80)
        print("6. ANALISE DE ANOS (data_inicio / data_final)")
        print("=" * 80)
        analyze_anos(df)
        
        print()
        print("=" * 80)
        print("7. MAPEAMENTO DE COLUNAS (RAW → NORM)")
        print("=" * 80)
        analyze_column_mapping(df)
        
        print()
        print("=" * 80)
        print("8. DUPLICATAS POTENCIAIS")
        print("=" * 80)
        analyze_duplicates(df)
        
        print()
        print("=" * 80)
        print("9. VALORES NUMERICOS (conversao de tipos)")
        print("=" * 80)
        analyze_numeric_fields(df)
        
        print()
        print("=" * 80)
        print("ANALISE CONCLUIDA")
        print("=" * 80)
        print()
        print("Use estas informacoes para tomar decisoes em docs/normalization_decisions.md")
        
    except Exception as e:
        print(f"ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def analyze_nulls(df):
    """Analisa distribuição de valores NULL."""
    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df)) * 100
    
    print(f"{'Coluna':<30} {'NULLs':<10} {'%':<10}")
    print("-" * 50)
    
    for col in df.columns:
        if null_counts[col] > 0:
            print(f"{col:<30} {null_counts[col]:>8} {null_pct[col]:>8.1f}%")
    
    if null_counts.sum() == 0:
        print("Nenhum valor NULL encontrado")


def analyze_marcas(df):
    """Analisa variações de marcas."""
    if 'marca' not in df.columns:
        print("Coluna 'marca' nao encontrada")
        return
    
    marcas = df['marca'].dropna().astype(str).str.strip()
    marca_counts = marcas.value_counts()
    
    print(f"Total de marcas unicas: {len(marca_counts)}")
    print()
    print("Top 20 marcas:")
    print(f"{'Marca':<30} {'Quantidade':<15} {'%':<10}")
    print("-" * 55)
    
    for marca, count in marca_counts.head(20).items():
        pct = (count / len(df)) * 100
        print(f"{marca:<30} {count:>12} {pct:>8.1f}%")
    
    print()
    print("Todas as marcas (para identificar variacoes):")
    print("-" * 50)
    for marca in sorted(marca_counts.index):
        count = marca_counts[marca]
        print(f"  {marca:<40} ({count:>5} registros)")


def analyze_combustiveis(df):
    """Analisa variações de combustíveis."""
    if 'combustivel' not in df.columns:
        print("Coluna 'combustivel' nao encontrada")
        return
    
    combustiveis = df['combustivel'].dropna().astype(str).str.strip()
    comb_counts = combustiveis.value_counts()
    
    print(f"Total de combustiveis unicos: {len(comb_counts)}")
    print()
    print("Todos os combustiveis:")
    print(f"{'Combustivel':<30} {'Quantidade':<15} {'%':<10}")
    print("-" * 55)
    
    for comb, count in comb_counts.items():
        pct = (count / len(df)) * 100
        print(f"{comb:<30} {count:>12} {pct:>8.1f}%")


def analyze_transmissoes(df):
    """Analisa variações de transmissões."""
    # Verificar ambas as colunas
    trans_cols = []
    if 'tipo_transmissao' in df.columns:
        trans_cols.append('tipo_transmissao')
    
    if not trans_cols:
        print("Colunas de transmissao nao encontradas")
        return
    
    for col in trans_cols:
        print(f"\nColuna: {col}")
        transmissoes = df[col].dropna().astype(str).str.strip()
        trans_counts = transmissoes.value_counts()
        
        print(f"Total de valores unicos: {len(trans_counts)}")
        print(f"{'Valor':<30} {'Quantidade':<15} {'%':<10}")
        print("-" * 55)
        
        for trans, count in trans_counts.items():
            pct = (count / len(df)) * 100
            print(f"{trans:<30} {count:>12} {pct:>8.1f}%")


def analyze_aspiracao(df):
    """Analisa variações de aspiração."""
    if 'aspiracao' not in df.columns:
        print("Coluna 'aspiracao' nao encontrada")
        return
    
    aspiracao = df['aspiracao'].dropna().astype(str).str.strip()
    asp_counts = aspiracao.value_counts()
    
    print(f"Total de valores unicos: {len(asp_counts)}")
    print(f"{'Aspiracao':<30} {'Quantidade':<15} {'%':<10}")
    print("-" * 55)
    
    for asp, count in asp_counts.items():
        pct = (count / len(df)) * 100
        print(f"{asp:<30} {count:>12} {pct:>8.1f}%")


def analyze_anos(df):
    """Analisa padrões de anos."""
    print("Coluna: data_inicio")
    if 'data_inicio' in df.columns:
        data_inicio = df['data_inicio'].dropna()
        print(f"  Total preenchido: {len(data_inicio)} ({len(data_inicio)/len(df)*100:.1f}%)")
        if len(data_inicio) > 0:
            print(f"  Min: {data_inicio.min()}")
            print(f"  Max: {data_inicio.max()}")
            print(f"  Valores unicos: {data_inicio.nunique()}")
    else:
        print("  Coluna nao encontrada")
    
    print()
    print("Coluna: data_final")
    if 'data_final' in df.columns:
        data_final = df['data_final'].dropna()
        print(f"  Total preenchido: {len(data_final)} ({len(data_final)/len(df)*100:.1f}%)")
        if len(data_final) > 0:
            print(f"  Min: {data_final.min()}")
            print(f"  Max: {data_final.max()}")
            print(f"  Valores unicos: {data_final.nunique()}")
    else:
        print("  Coluna nao encontrada")
    
    print()
    print("Analise de ranges:")
    if 'data_inicio' in df.columns and 'data_final' in df.columns:
        # Filtrar apenas registros com ambos preenchidos
        both_filled = df.dropna(subset=['data_inicio', 'data_final'])
        print(f"  Registros com ambos preenchidos: {len(both_filled)}")
        
        if len(both_filled) > 0:
            # Calcular diferença
            both_filled = both_filled.copy()
            both_filled['diff_anos'] = both_filled['data_final'].astype(float) - both_filled['data_inicio'].astype(float)
            
            print(f"  Range medio: {both_filled['diff_anos'].mean():.1f} anos")
            print(f"  Range min: {both_filled['diff_anos'].min():.0f} anos")
            print(f"  Range max: {both_filled['diff_anos'].max():.0f} anos")
            
            # Contar ranges > 1 ano
            ranges_gt_1 = (both_filled['diff_anos'] > 1).sum()
            print(f"  Ranges > 1 ano: {ranges_gt_1} ({ranges_gt_1/len(both_filled)*100:.1f}%)")


def analyze_column_mapping(df):
    """Analisa mapeamento de colunas RAW → NORM."""
    print("Colunas disponiveis na RAW:")
    for col in sorted(df.columns):
        if col not in ['id_raw', 'load_timestamp', 'source_file', 'excel_row']:
            unique_count = df[col].nunique()
            null_count = df[col].isnull().sum()
            print(f"  - {col:<30} (unicos: {unique_count:>5}, nulls: {null_count:>5})")
    
    print()
    print("Mapeamento sugerido RAW → NORM:")
    print("  marca → marca")
    print("  modelo_veiculo → modelo")
    print("  nome_veiculo → versao? (verificar valores)")
    print("  nome_motor → motor? (verificar valores)")
    print("  tipo_transmissao → transmissao")
    print("  combustivel → combustivel")
    print("  aspiracao → aspiracao")
    
    print()
    print("Amostras para validar mapeamento:")
    if 'nome_veiculo' in df.columns:
        print(f"\n  nome_veiculo (primeiros 10 valores unicos):")
        for val in df['nome_veiculo'].dropna().unique()[:10]:
            print(f"    - {val}")
    
    if 'modelo_veiculo' in df.columns:
        print(f"\n  modelo_veiculo (primeiros 10 valores unicos):")
        for val in df['modelo_veiculo'].dropna().unique()[:10]:
            print(f"    - {val}")


def analyze_duplicates(df):
    """Analisa duplicatas potenciais."""
    print("Analisando duplicatas por codigo_fraga...")
    if 'codigo_fraga' in df.columns:
        codigo_counts = df['codigo_fraga'].value_counts()
        duplicates = codigo_counts[codigo_counts > 1]
        if len(duplicates) > 0:
            print(f"  Codigos duplicados: {len(duplicates)}")
            print(f"  Total de registros duplicados: {duplicates.sum()}")
            print(f"  Exemplos:")
            for codigo, count in duplicates.head(5).items():
                print(f"    - {codigo}: {count} registros")
        else:
            print("  Nenhum codigo_fraga duplicado")
    
    print()
    print("Analisando combinacoes potenciais para hash...")
    # Verificar quantas combinações únicas teríamos com campos básicos
    hash_fields = ['marca', 'modelo_veiculo', 'nome_veiculo', 'nome_motor', 
                   'combustivel', 'tipo_transmissao', 'aspiracao']
    available_fields = [f for f in hash_fields if f in df.columns]
    
    if len(available_fields) >= 3:
        # Criar combinação para análise
        df_clean = df[available_fields].fillna('')
        df_clean['combinacao'] = df_clean[available_fields].apply(
            lambda x: '|'.join(x.astype(str)), axis=1
        )
        unique_combos = df_clean['combinacao'].nunique()
        print(f"  Campos testados: {', '.join(available_fields)}")
        print(f"  Combinacoes unicas: {unique_combos:,}")
        print(f"  Total registros: {len(df):,}")
        print(f"  Taxa de unicidade: {unique_combos/len(df)*100:.1f}%")
        
        # Verificar duplicatas
        combo_counts = df_clean['combinacao'].value_counts()
        duplicates = combo_counts[combo_counts > 1]
        if len(duplicates) > 0:
            print(f"  Combinacoes duplicadas: {len(duplicates)}")
            print(f"  Total registros em duplicatas: {duplicates.sum()}")


def analyze_numeric_fields(df):
    """Analisa campos que podem ser convertidos para numérico."""
    numeric_candidates = {
        'cilindrada_litros': 'NUMERIC',
        'numero_cilindros': 'INTEGER',
        'potencia_cv': 'INTEGER',
        'data_inicio': 'INTEGER',
        'data_final': 'INTEGER'
    }
    
    for col, expected_type in numeric_candidates.items():
        if col in df.columns:
            print(f"\nColuna: {col} (esperado: {expected_type})")
            non_null = df[col].dropna()
            
            if len(non_null) == 0:
                print("  Todos os valores sao NULL")
                continue
            
            # Tentar converter
            if expected_type == 'INTEGER':
                try:
                    numeric_vals = pd.to_numeric(non_null.astype(str), errors='coerce')
                    valid = numeric_vals.notna().sum()
                    invalid = len(non_null) - valid
                    print(f"  Valores validos: {valid} ({valid/len(non_null)*100:.1f}%)")
                    print(f"  Valores invalidos: {invalid} ({invalid/len(non_null)*100:.1f}%)")
                    
                    if invalid > 0:
                        # Mostrar valores inválidos
                        invalid_vals = non_null[~numeric_vals.notna()]
                        print(f"  Exemplos de valores invalidos:")
                        for val in invalid_vals.unique()[:5]:
                            print(f"    - '{val}'")
                    
                    if valid > 0:
                        print(f"  Min: {numeric_vals.min():.0f}")
                        print(f"  Max: {numeric_vals.max():.0f}")
                except Exception as e:
                    print(f"  Erro ao converter: {e}")
            else:  # NUMERIC
                try:
                    numeric_vals = pd.to_numeric(non_null.astype(str), errors='coerce')
                    valid = numeric_vals.notna().sum()
                    invalid = len(non_null) - valid
                    print(f"  Valores validos: {valid} ({valid/len(non_null)*100:.1f}%)")
                    print(f"  Valores invalidos: {invalid} ({invalid/len(non_null)*100:.1f}%)")
                    
                    if invalid > 0:
                        invalid_vals = non_null[~numeric_vals.notna()]
                        print(f"  Exemplos de valores invalidos:")
                        for val in invalid_vals.unique()[:5]:
                            print(f"    - '{val}'")
                    
                    if valid > 0:
                        print(f"  Min: {numeric_vals.min():.2f}")
                        print(f"  Max: {numeric_vals.max():.2f}")
                except Exception as e:
                    print(f"  Erro ao converter: {e}")


def main():
    """Função principal."""
    analyze_raw_data()


if __name__ == "__main__":
    main()

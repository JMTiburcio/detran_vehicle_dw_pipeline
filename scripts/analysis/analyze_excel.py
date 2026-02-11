"""
Script temporário para analisar o arquivo Excel da Fraga e sugerir estrutura da tabela raw.

Este script analisa data/input/fraga_veiculos_v2.xlsx e gera:
- Estrutura do arquivo (colunas, tipos de dados)
- Amostras de dados
- Sugestão de CREATE TABLE SQL para staging.fraga_vehicle_raw
"""

import pandas as pd
import sys
import unicodedata
import re
from pathlib import Path
from typing import Dict, Any

# Caminho do arquivo Excel (relativo à raiz do projeto)
# Script deve ser executado da raiz: python scripts/analysis/analyze_excel.py
EXCEL_FILE = Path(__file__).parent.parent.parent / "data/input/fraga_veiculos_v2.xlsx"


def analyze_excel_structure(file_path: Path) -> Dict[str, Any]:
    """
    Analisa a estrutura do arquivo Excel.
    
    Returns:
        Dict com informações sobre o arquivo
    """
    print(f"\n{'='*80}")
    print(f"ANALISE DO ARQUIVO EXCEL: {file_path.name}")
    print(f"{'='*80}\n")
    
    # Verificar se arquivo existe
    if not file_path.exists():
        print(f"ERRO: Arquivo não encontrado: {file_path}")
        sys.exit(1)
    
    # Ler Excel - tentar detectar automaticamente
    try:
        # Primeiro, listar todas as abas
        xl_file = pd.ExcelFile(file_path)
        print(f"Planilhas encontradas: {xl_file.sheet_names}")
        print(f"Usando primeira planilha: '{xl_file.sheet_names[0]}'\n")
        
        # Ler primeira planilha
        df = pd.read_excel(file_path, sheet_name=0)
        
    except Exception as e:
        print(f"ERRO ao ler arquivo Excel: {e}")
        sys.exit(1)
    
    # Informações gerais
    print(f"{'-'*80}")
    print("INFORMACOES GERAIS")
    print(f"{'-'*80}")
    print(f"Total de linhas: {len(df):,}")
    print(f"Total de colunas: {len(df.columns)}")
    print(f"Planilha: {xl_file.sheet_names[0]}")
    print()
    
    # Estrutura de colunas
    print(f"{'-'*80}")
    print("ESTRUTURA DE COLUNAS")
    print(f"{'-'*80}")
    print(f"{'Coluna':<30} {'Tipo Python':<20} {'Tipo Sugerido SQL':<25} {'Nulos':<10} {'Valores Únicos':<15}")
    print(f"{'-'*100}")
    
    column_info = []
    for col in df.columns:
        # Tipo Python
        python_type = str(df[col].dtype)
        
        # Tipo SQL sugerido
        sql_type = suggest_sql_type(df[col])
        
        # Contagem de nulos
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100
        
        # Valores únicos
        unique_count = df[col].nunique()
        
        print(f"{col:<30} {python_type:<20} {sql_type:<25} {null_count:>6} ({null_pct:>5.1f}%) {unique_count:>10}")
        
        column_info.append({
            'name': col,
            'python_type': python_type,
            'sql_type': sql_type,
            'null_count': null_count,
            'null_pct': null_pct,
            'unique_count': unique_count,
            'sample_values': df[col].dropna().head(5).tolist()
        })
    
    print()
    
    # Amostra de dados
    print(f"{'-'*80}")
    print("AMOSTRA DE DADOS (primeiras 5 linhas)")
    print(f"{'-'*80}")
    print(df.head().to_string())
    print()
    
    # Estatísticas adicionais
    print(f"{'-'*80}")
    print("ESTATISTICAS ADICIONAIS")
    print(f"{'-'*80}")
    print(f"Linhas completamente nulas: {df.isna().all(axis=1).sum()}")
    print(f"Colunas completamente nulas: {df.isna().all(axis=0).sum()}")
    print()
    
    return {
        'dataframe': df,
        'columns': column_info,
        'sheet_name': xl_file.sheet_names[0],
        'total_rows': len(df),
        'total_columns': len(df.columns)
    }


def normalize_column_name(col_name: str) -> str:
    """
    Normaliza nome de coluna para ser válido em SQL.
    Remove acentos, espaços, caracteres especiais.
    """
    # Converter para minúsculas
    normalized = col_name.lower()
    
    # Remover acentos
    normalized = unicodedata.normalize('NFD', normalized)
    normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    
    # Substituir espaços e hífens por underscore
    normalized = re.sub(r'[\s\-]+', '_', normalized)
    
    # Remover caracteres especiais, manter apenas alfanuméricos e underscore
    normalized = re.sub(r'[^a-z0-9_]', '', normalized)
    
    # Remover underscores múltiplos
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remover underscore no início e fim
    normalized = normalized.strip('_')
    
    # Se vazio, usar nome genérico
    if not normalized:
        normalized = 'coluna_' + str(hash(col_name) % 10000)
    
    return normalized


def suggest_sql_type(series: pd.Series) -> str:
    """
    Sugere tipo SQL baseado no tipo e conteúdo da série.
    """
    # Se todos são nulos, usar VARCHAR genérico
    if series.isna().all():
        return "VARCHAR(255)"
    
    # Remover nulos para análise
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return "VARCHAR(255)"
    
    # Verificar tipo Python
    dtype = str(series.dtype)
    
    # Numéricos
    if dtype.startswith('int'):
        # Verificar range para escolher INT vs BIGINT
        max_val = non_null.max()
        min_val = non_null.min()
        if max_val > 2147483647 or min_val < -2147483648:
            return "BIGINT"
        return "INTEGER"
    
    if dtype.startswith('float'):
        # Verificar se são inteiros (anos, por exemplo)
        if (non_null % 1 == 0).all():
            max_val = non_null.max()
            min_val = non_null.min()
            if max_val > 2147483647 or min_val < -2147483648:
                return "BIGINT"
            return "INTEGER"
        return "NUMERIC(18,2)"
    
    # Datas
    if dtype.startswith('datetime'):
        return "TIMESTAMP"
    
    # Strings
    if dtype == 'object':
        # Verificar se são números como string
        str_values = non_null.astype(str)
        
        # Tentar converter para numérico
        try:
            numeric_values = pd.to_numeric(str_values, errors='coerce')
            if numeric_values.notna().sum() / len(non_null) > 0.9:  # 90% são numéricos
                # Se são inteiros
                if (numeric_values % 1 == 0).all():
                    max_val = numeric_values.max()
                    min_val = numeric_values.min()
                    if max_val > 2147483647 or min_val < -2147483648:
                        return "BIGINT"
                    return "INTEGER"
                else:
                    return "NUMERIC(18,2)"
        except:
            pass
        
        # Verificar tamanho máximo
        max_length = str_values.str.len().max()
        
        # Sugerir tamanho com margem
        if max_length <= 50:
            return f"VARCHAR(100)"
        elif max_length <= 255:
            return f"VARCHAR(255)"
        elif max_length <= 500:
            return f"VARCHAR(500)"
        else:
            return f"VARCHAR(1000)"
    
    # Boolean
    if dtype == 'bool':
        return "BOOLEAN"
    
    # Default
    return "VARCHAR(255)"


def generate_sql_suggestion(analysis: Dict[str, Any]) -> str:
    """
    Gera sugestão de CREATE TABLE SQL baseada na análise.
    """
    print(f"{'-'*80}")
    print("SUGESTAO DE CREATE TABLE SQL")
    print(f"{'-'*80}\n")
    
    sql = "-- Create raw table for Excel data\n"
    sql += "-- This table stores data exactly as it comes from Excel\n"
    sql += "-- Generated from analysis of fraga_veiculos_v2.xlsx\n\n"
    sql += "CREATE TABLE IF NOT EXISTS staging.fraga_vehicle_raw (\n"
    sql += "    id_raw SERIAL PRIMARY KEY,\n"
    
    # Adicionar colunas do Excel
    for i, col_info in enumerate(analysis['columns']):
        col_name = normalize_column_name(col_info['name'])
        sql_type = col_info['sql_type']
        
        # Adicionar NOT NULL se não tiver nulos
        nullable = "" if col_info['null_count'] == 0 else ""
        
        sql += f"    {col_name} {sql_type}{nullable}"
        
        # Adicionar comentário com nome original se diferente
        original_name = col_info['name']
        if col_name != normalize_column_name(original_name) or original_name != col_name:
            # Escapar aspas no comentário
            original_escaped = original_name.replace("'", "''")
            sql += f",  -- Original: {original_escaped}"
        else:
            sql += ","
        
        sql += "\n"
    
    # Colunas de metadados (em inglês)
    sql += "    -- Metadata columns (English)\n"
    sql += "    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
    sql += "    source_file VARCHAR(500),\n"
    sql += "    excel_row INTEGER\n"
    sql += ");\n\n"
    
    sql += f"\nCOMMENT ON TABLE staging.fraga_vehicle_raw IS 'Raw data from Fraga Excel files, stored exactly as received (Total rows analyzed: {analysis['total_rows']:,})';\n\n"
    
    # Índices sugeridos
    sql += "-- Indexes for faster lookups\n"
    
    # Procurar coluna que pode ser código/chave
    for col_info in analysis['columns']:
        col_name = normalize_column_name(col_info['name'])
        
        # Se parece ser uma chave (baixa duplicação ou nome sugere chave)
        # Excluir codigo_fipe conforme solicitado
        if ('codigo' in col_name or 'id' in col_name or 'key' in col_name) and col_info['unique_count'] > 0 and col_name != 'codigo_fipe':
            sql += f"CREATE INDEX IF NOT EXISTS idx_raw_{col_name} ON staging.fraga_vehicle_raw({col_name});\n"
    
    sql += "CREATE INDEX IF NOT EXISTS idx_raw_load_timestamp ON staging.fraga_vehicle_raw(load_timestamp);\n"
    
    return sql


def main():
    """Função principal."""
    try:
        # Analisar arquivo
        analysis = analyze_excel_structure(EXCEL_FILE)
        
        # Gerar sugestão SQL
        sql_suggestion = generate_sql_suggestion(analysis)
        
        print(sql_suggestion)
        
        # Salvar SQL em arquivo
        output_file = Path("sql_suggestion_raw_table.sql")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sql_suggestion)
        
        print(f"\n{'-'*80}")
        print(f"Sugestao SQL salva em: {output_file}")
        print(f"{'-'*80}\n")
        
        # Mostrar exemplos de valores por coluna
        print(f"{'-'*80}")
        print("EXEMPLOS DE VALORES POR COLUNA")
        print(f"{'-'*80}\n")
        
        for col_info in analysis['columns'][:10]:  # Primeiras 10 colunas
            print(f"Coluna: {col_info['name']}")
            print(f"  Exemplos: {col_info['sample_values'][:3]}")
            print()
        
        if len(analysis['columns']) > 10:
            print(f"... e mais {len(analysis['columns']) - 10} colunas\n")
        
    except Exception as e:
        print(f"\nERRO durante análise: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

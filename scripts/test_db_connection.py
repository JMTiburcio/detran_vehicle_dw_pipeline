"""
Script para testar conexão com o banco de dados PostgreSQL.

Este script verifica se as variáveis de ambiente estão configuradas
e testa a conexão com o banco de dados.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

# Carregar variáveis de ambiente
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# Variáveis de ambiente necessárias
REQUIRED_ENV_VARS = [
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD"
]


def check_env_vars():
    """Verifica se todas as variáveis de ambiente necessárias estão definidas."""
    print("=" * 80)
    print("VERIFICANDO VARIAVEIS DE AMBIENTE")
    print("=" * 80)
    print()
    
    missing_vars = []
    env_vars = {}
    
    for var in REQUIRED_ENV_VARS:
        value = os.getenv(var)
        if value is None or value == "":
            missing_vars.append(var)
            print(f"[X] {var}: NAO DEFINIDA")
        else:
            # Mascarar senha
            if var == "DB_PASSWORD" or var == "DB_USER" or var == "DB_HOST":
                display_value = "*" * len(value) if value else "NAO DEFINIDA"
            else:
                display_value = value
            env_vars[var] = value
            print(f"[OK] {var}: {display_value}")
    
    print()
    
    if missing_vars:
        print("=" * 80)
        print("ERRO: Variáveis de ambiente faltando:")
        print("=" * 80)
        for var in missing_vars:
            print(f"  - {var}")
        print()
        print("Por favor, configure o arquivo .env na raiz do projeto.")
        print("Você pode copiar config/env.example para .env e preencher os valores.")
        return None
    
    return env_vars


def test_connection(env_vars):
    """Testa a conexão com o banco de dados."""
    print("=" * 80)
    print("TESTANDO CONEXAO COM BANCO DE DADOS")
    print("=" * 80)
    print()
    
    try:
        print(f"Conectando a ************:{env_vars['DB_PORT']}/{env_vars['DB_NAME']}...")
        print(f"Usuário: ************")
        print()
        
        # Tentar conexão
        conn = psycopg2.connect(
            host=env_vars['DB_HOST'],
            port=env_vars['DB_PORT'],
            database=env_vars['DB_NAME'],
            user=env_vars['DB_USER'],
            password=env_vars['DB_PASSWORD'],
            connect_timeout=5
        )
        
        # Testar query simples
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.execute("SELECT current_database();")
        database = cursor.fetchone()[0]
        
        cursor.execute("SELECT current_user;")
        user = cursor.fetchone()[0]
        
        # Verificar se schemas existem
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('staging', 'core', 'analytics')
            ORDER BY schema_name;
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        print("[OK] CONEXAO ESTABELECIDA COM SUCESSO!")
        print()
        print("Informacoes do banco:")
        print(f"  - Versao PostgreSQL: {version.split(',')[0]}")
        print(f"  - Database: {database}")
        print(f"  - Usuario: {user}")
        print()
        
        if schemas:
            print("Schemas encontrados:")
            for schema in schemas:
                print(f"  [OK] {schema}")
        else:
            print("[!] Schemas (staging, core, analytics) ainda nao foram criados.")
            print("   Execute os scripts SQL em sql/staging/, sql/core/ e sql/analytics/")
        
        print()
        return True
        
    except OperationalError as e:
        print("[X] ERRO AO CONECTAR:")
        print(f"   {str(e)}")
        print()
        print("Possiveis causas:")
        print("  - Banco de dados nao esta rodando")
        print("  - Credenciais incorretas")
        print("  - Host/porta incorretos")
        print("  - Database nao existe")
        return False
        
    except Exception as e:
        print("[X] ERRO INESPERADO:")
        print(f"   {type(e).__name__}: {str(e)}")
        return False


def main():
    """Função principal."""
    print()
    print("=" * 80)
    print("TESTE DE CONEXÃO COM BANCO DE DADOS")
    print("=" * 80)
    print()
    
    # Verificar se arquivo .env existe
    env_file = Path(__file__).parent.parent / ".env"
    if not env_file.exists():
        print("[!] Arquivo .env nao encontrado!")
        print()
        print("Para criar o arquivo .env:")
        print("  1. Copie config/env.example para .env na raiz do projeto")
        print("  2. Preencha as variaveis de ambiente com suas credenciais")
        print()
        print("Variaveis necessarias:")
        for var in REQUIRED_ENV_VARS:
            print(f"  - {var}")
        print()
        sys.exit(1)
    
    # Verificar variáveis de ambiente
    env_vars = check_env_vars()
    if env_vars is None:
        sys.exit(1)
    
    # Testar conexão
    success = test_connection(env_vars)
    
    if success:
        print("=" * 80)
        print("[OK] TESTE CONCLUIDO COM SUCESSO!")
        print("=" * 80)
        sys.exit(0)
    else:
        print("=" * 80)
        print("[X] TESTE FALHOU")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import pyodbc
import os
from dotenv import load_dotenv

# Carregar .env (apenas para o PostgreSQL)
load_dotenv()

# Conexão com PostgreSQL
POSTGRES_CONN = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cursor_pg = POSTGRES_CONN.cursor()

# Conexão com SQL Server (dados do .env)
SQLSERVER_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQLSERVER_HOST')};"
    f"DATABASE={os.getenv('SQLSERVER_DATABASE')};"
    f"UID={os.getenv('SQLSERVER_USER')};"
    f"PWD={os.getenv('SQLSERVER_PASSWORD')}"
)
conn_sql = pyodbc.connect(SQLSERVER_CONN_STR)
cursor_sql = conn_sql.cursor()

# 1. Buscar o maior idcia já inserido no PostgreSQL
cursor_pg.execute("SELECT COALESCE(MAX(idorigem_destino), 0) FROM origem_destino")
ultimo_id_pg = cursor_pg.fetchone()[0]

# 2. Buscar dados novos no SQL Server com id acima do último inserido
cursor_sql.execute("""
    Select 
    IdOrigem_Destino as idorigem_destino, 
    Nome as nome
    From
    cad_Origem_Destino
    Where
    Ativo = 1
    and
    IdOrigem_Destino > ?
    """, (ultimo_id_pg,))

# 3. Coletar os dados
novos_dados = cursor_sql.fetchall()

# 4. Preparar dados para insert no PostgreSQL
dados_para_inserir = [(int(row.idorigem_destino), row.nome) for row in novos_dados]

# 5. Executar insert em lote
if dados_para_inserir:
    execute_batch(cursor_pg, """
        INSERT INTO origem_destino (idorigem_destino, nome)
        VALUES (%s, %s)
        ON CONFLICT (idorigem_destino) DO NOTHING;
    """, dados_para_inserir)
    POSTGRES_CONN.commit()
    print(f"Inseridos {len(dados_para_inserir)} novos registros na tabela 'origem_destino'.")
else:
    print("Nenhum novo registro para inserir.")

# 6. Fechar conexões
cursor_sql.close()
conn_sql.close()
cursor_pg.close()
POSTGRES_CONN.close()

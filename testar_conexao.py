import os
from dotenv import load_dotenv
import pyodbc
import psycopg2

load_dotenv()

SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('SQLSERVER_HOST')};"
    f"DATABASE={os.getenv('SQLSERVER_DATABASE')};"
    f"UID={os.getenv('SQLSERVER_USER')};"
    f"PWD={os.getenv('SQLSERVER_PASSWORD')};"
)

def testar_conexoes():
    try:
        conn_sql = pyodbc.connect(SQLSERVER_CONN_STR)
        print("Conexão com SQL Server: OK")
        conn_sql.close()
    except Exception as e:
        print("Erro ao conectar no SQL Server:", e)

    try:
        conn_pg = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        print("Conexão com PostgreSQL: OK")
        conn_pg.close()
    except Exception as e:
        print("Erro ao conectar no PostgreSQL:", e)

if __name__ == "__main__":
    testar_conexoes()

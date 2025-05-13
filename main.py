# main.py
import os
from dotenv import load_dotenv
from read_email import processar_emails
from insert_sqlserver import inserir_sqlserver


# Carrega variáveis de ambiente
load_dotenv()

if __name__ == "__main__":
    print("🚀 Iniciando processamento de e-mails e inserção no banco de dados...")

    # 1. Lê os e-mails e salva no PostgreSQL
    dados_inseridos = processar_emails()

    # 2. Se houver dados, insere no SQL Server
    if dados_inseridos:
        print("📥 Dados processados. Iniciando inserção no SQL Server...")
        inserir_sqlserver()
        print("✅ Processo completo com sucesso!")
    else:
        print("⚠️ Nenhum dado foi processado. Nenhum e-mail novo com Excel encontrado.")
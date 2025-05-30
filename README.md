# email_fh

Este projeto realiza a automação de leitura de e-mails com anexos em Excel, extração de informações e inserção de dados em dois bancos de dados: PostgreSQL (trata os dados) e SQL Server (ERP).

## 📂 Estrutura do projeto

- `read_mail.py`: Lê os e-mails, identifica anexos Excel e extrai os dados relevantes.
- `main.py`: Orquestra o processo de execução (pode chamar os outros scripts).
- `insert_sqlserver.py`: Faz a inserção dos dados no banco de dados SQL Server.
- `.env`: Contém variáveis de ambiente (como senhas, conexões de banco).

## ⚙️ Como usar

1. Clone o repositório:
   ```bash
   git clone https://github.com/pedrofariasgit/email_fh.git
   cd email_fh

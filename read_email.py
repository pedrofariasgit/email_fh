import msal
import requests
import pandas as pd
import base64
import io
import psycopg2
import pyodbc
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Carregar .env
load_dotenv()

# Conexoes
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=kpm.sql.headcargo.com.br,9322;"
    "DATABASE=HeadCARGO_KPM_HOMOLOGACAO;"
    "UID=hc_kpm_ti;"
    "PWD=" + os.getenv("SQLSERVER_PASSWORD")
)

POSTGRES_CONN = psycopg2.connect(
    host="89.117.17.6",
    port=5432,
    database="kpm_freehand",
    user="kpm",
    password="@Kpm<102030>"
)
cursor_pg = POSTGRES_CONN.cursor()

# Mapas fixos
operation_map = {"impo": 2, "expo": 1}
modality_map = {"air": 1, "sea": 2}
cargo_type_map = {"em branco": 0, "air": 1, "break-bulk": 2, "fcl": 3, "lcl": 4, "ro-ro": 5}
coin_map = {"usd": 31, "eur": 52, "brl": 110}
incoterm_map = {"cif": 1, "fob": 2, "exw": 3, "ddp": 4, "fca": 5, "fas": 6, "cfr": 7, "cpt": 8, "cip": 9, "dat": 10, "dap": 11, "fot": 12}
equip_map = {
    "20 tank": 1, "20 plataform": 2, "20 dry box": 3, "20 open top": 4, "20 flat rack": 5,
    "20 reefer": 6, "40 plataform": 7, "40 flat rack": 8, "40 high cube": 9, "40 dry box": 10,
    "40 open top": 11, "40 reefer": 12, "20 nor": 22
}

def mapear(valor, mapa):
    if not valor:
        return None
    return mapa.get(str(valor).strip().lower())

def buscar_id_sqlserver(nome, tipo):
    conn_sql = pyodbc.connect(SQLSERVER_CONN_STR)
    cursor_sql = conn_sql.cursor()

    if tipo == "origin" or tipo == "destination":
        query = """
        SELECT TOP 1 IdOrigem_Destino FROM cad_Origem_Destino
        WHERE UPPER(LTRIM(RTRIM(Nome))) LIKE ? AND Ativo = 1 AND Tipo = 1
        ORDER BY IdOrigem_Destino
        """

    elif tipo == "cliente":
        query = """
        SELECT TOP 1 p.IdPessoa
        FROM cad_Pessoa p
        INNER JOIN cad_Cliente c ON c.IdPessoa = p.IdPessoa
        WHERE p.Nome COLLATE Latin1_General_CI_AI LIKE ?
        AND p.Ativo = 1
        ORDER BY p.IdPessoa DESC
        """

    elif tipo == "shipowner":
        query = """
        SELECT TOP 1 p.IdPessoa
        FROM cad_Pessoa p
        INNER JOIN cad_Companhia_Transporte c on c.IdPessoa = p.IdPessoa
        WHERE UPPER(LTRIM(RTRIM(p.Nome))) LIKE ? AND p.Ativo = 1
        """
    else:
        return None

    cursor_sql.execute(query, f"{nome.strip().upper()}%")
    result = cursor_sql.fetchone()
    conn_sql.close()
    return result[0] if result else None

def processar_emails() -> list[dict]:
    dados_processados = []

    CLIENT_ID = os.getenv("CLIENT_ID")
    TENANT_ID = os.getenv("TENANT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )

    token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

    if "access_token" not in token_response:
        print(f"❌ Erro ao obter token: {token_response.get('error_description')}")
        return []

    headers = {"Authorization": f"Bearer {token_response['access_token']}", "Accept": "application/json"}
    print("✅ Token obtido com sucesso.")

    ontem = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    ontem_iso = ontem.isoformat() + "Z"

    url = f"https://graph.microsoft.com/v1.0/users/freehand.reefer@kpmlogistics.com/mailFolders/inbox/messages?$filter=receivedDateTime ge {ontem_iso}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Erro ao acessar e-mails: {response.status_code}")
        return []

    emails = response.json().get("value", [])
    emails_com_anexo = [email for email in emails if email.get("hasAttachments", False)]

    for email in emails_com_anexo:
        attachments_url = f"https://graph.microsoft.com/v1.0/users/freehand.reefer@kpmlogistics.com/messages/{email['id']}/attachments"
        attachments_response = requests.get(attachments_url, headers=headers)

        if attachments_response.status_code != 200:
            continue

        for attachment in attachments_response.json().get("value", []):
            if attachment["name"].lower().startswith('freehand') and attachment["name"].endswith(('.xlsx', '.xls')):
                try:
                    content_bytes = base64.b64decode(attachment["contentBytes"])
                    excel_file = io.BytesIO(content_bytes)
                    df = pd.read_excel(excel_file)
                    registros = df.to_dict('records')

                    def limpar(texto):
                        return str(texto).strip() if texto and str(texto).strip().lower() != "nan" else None

                    for registro in registros:
                        operation_raw = registro.get("Operation")
                        modality_raw = registro.get("Modality")
                        origin_raw = registro.get("Origin")
                        destination_raw = registro.get("Destination")
                        shipowner_raw = registro.get("Shipowner")
                        client_raw = registro.get("Client")
                        shipper_raw = registro.get("Shipper")
                        consignee_raw = registro.get("Consignee")
                        temperature_raw = registro.get("Temperature")

                        cargo_type_raw = registro.get("Cargo_Type")
                        coin_raw = registro.get("Coin")
                        incoterm_raw = registro.get("Incoterm")
                        equip_raw = registro.get("Equip")

                        operation = limpar(operation_raw)
                        modality = limpar(modality_raw)
                        origin = limpar(origin_raw)
                        destination = limpar(destination_raw)
                        shipowner = limpar(shipowner_raw)
                        client = limpar(client_raw)
                        cargo_type = limpar(cargo_type_raw)
                        coin = limpar(coin_raw)
                        incoterm = limpar(incoterm_raw)
                        equip = limpar(equip_raw)
                        shipper = limpar(shipper_raw)
                        consignee = limpar(consignee_raw)
                        try:
                            temperature = float(str(temperature_raw).replace(",", ".").replace("−", "-").strip()) if temperature_raw else None
                        except:
                            temperature = None


                        operation_id = mapear(operation, operation_map)
                        modality_id = mapear(modality, modality_map)
                        cargo_type_id = mapear(cargo_type, cargo_type_map)
                        coin_id = mapear(coin, coin_map)
                        incoterm_id = mapear(incoterm, incoterm_map)
                        equip_id = mapear(equip, equip_map)

                        origin_id = buscar_id_sqlserver(origin, "origin")
                        destination_id = buscar_id_sqlserver(destination, "destination")
                        client_id = buscar_id_sqlserver(client, "cliente")
                        shipowner_id = buscar_id_sqlserver(shipowner, "shipowner")
                        shipper_id = buscar_id_sqlserver(shipper, "cliente")
                        consignee_id = buscar_id_sqlserver(consignee, "cliente")

                        cursor_pg.execute("""
                            INSERT INTO freehand (
                            operation, operation_id, modality, modality_id, origin, origin_id, destination, destination_id,
                            shipowner, shipowner_id, client, client_id, shipper, shipper_id, consignee, consignee_id,
                            cargo_type, cargo_type_id, coin, coin_id, incoterm, incoterm_id, equip, equip_id,
                            temperature, upload_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s)

                        """, (
                            operation, operation_id, modality, modality_id,
                            origin_raw, origin_id, destination_raw, destination_id,
                            shipowner_raw, shipowner_id, client_raw, client_id,
                            shipper_raw, shipper_id, consignee_raw, consignee_id,
                            cargo_type, cargo_type_id, coin, coin_id,
                            incoterm, incoterm_id, equip, equip_id, temperature, datetime.now()
                        ))

                        dados_processados.append({
                            "operation_id": operation_id,
                            "modality_id": modality_id,
                            "origin_id": origin_id,
                            "destination_id": destination_id,
                            "shipowner_id": shipowner_id,
                            "client_id": client_id,
                            "cargo_type_id": cargo_type_id,
                            "coin_id": coin_id,
                            "incoterm_id": incoterm_id,
                            "equip_id": equip_id,
                            "shipper_id": shipper_id,
                            "consignee_id": consignee_id,
                            "temperature": temperature

                        })

                    POSTGRES_CONN.commit()
                    print("✅ Dados inseridos no PostgreSQL com sucesso!")

                except Exception as e:
                    POSTGRES_CONN.rollback()
                    print(f"❌ Erro ao processar Excel: {str(e)}")


    # Fechar conexao
    cursor_pg.close()
    POSTGRES_CONN.close()
    print("🔌 Conexão com o PostgreSQL encerrada.")
    return dados_processados


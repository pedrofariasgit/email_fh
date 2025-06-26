import os
import psycopg2
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
import psycopg2.extras


def inserir_sqlserver():
    # Carregar variáveis do .env
    load_dotenv()

    # Conexão PostgreSQL
    with psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    ) as POSTGRES_CONN:
        cursor_pg = POSTGRES_CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Conexão SQL Server
        SQLSERVER_CONN_STR = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={os.getenv('SQLSERVER_HOST')};"
            f"DATABASE={os.getenv('SQLSERVER_DATABASE')};"
            f"UID={os.getenv('SQLSERVER_USER')};"
            f"PWD={os.getenv('SQLSERVER_PASSWORD')};"
        )
        with pyodbc.connect(SQLSERVER_CONN_STR) as conn_sql:
            cursor_sql = conn_sql.cursor()

            cursor_pg.execute("SELECT * FROM freehand WHERE processed_hs = FALSE ORDER BY upload_date")
            dados = cursor_pg.fetchall()
            colunas = [desc[0] for desc in cursor_pg.description]

    for row in dados:
        dados_dict = dict(zip(colunas, row))

        operation = dados_dict['operation'].lower()
        modality = dados_dict['modality'].lower()

        if modality == 'air' and operation == 'impo': prefixo, id_tarefa = 'KPMIA', 6
        elif modality == 'air' and operation == 'expo': prefixo, id_tarefa = 'KPMEA', 7
        elif modality == 'sea' and operation == 'impo': prefixo, id_tarefa = 'KPMIMP', 8
        elif modality == 'sea' and operation == 'expo': prefixo, id_tarefa = 'KPMEXP', 2
        else: raise Exception("Combinação de operation/modality inválida")

        # Gera Numero_Processo
        cursor_sql.execute(f"""
            SELECT TOP 1
                CAST(SUBSTRING(Numero_Processo, {len(prefixo)+1}, CHARINDEX('/', Numero_Processo) - {len(prefixo)+1}) AS INT) AS Numero
            FROM mov_Logistica_House
            WHERE Numero_Processo LIKE '{prefixo}%'
            ORDER BY Numero DESC        
        """)
        resultado = cursor_sql.fetchone()
        ultimo_numero = resultado[0] if resultado and resultado[0] is not None else 0
        novo_numero = ultimo_numero + 1
        ano_atual = datetime.now().strftime('%y')
        numero_processo = f"{prefixo}{novo_numero}/{ano_atual}"

        # Numero_Processo da Master
        cursor_sql.execute("""
            SELECT TOP 1
                CAST(LEFT(Numero_Processo, CHARINDEX('/', Numero_Processo) - 1) AS INT) AS Numero
            FROM mov_Logistica_Master
            WHERE RIGHT(Numero_Processo, 3) = '/25'
            ORDER BY Numero DESC
        """)
        resultado = cursor_sql.fetchone()
        ultimo_num = resultado[0] if resultado and resultado[0] is not None else 0

        novo_num_m = ultimo_num + 1
        numero_processo_m = f"{novo_num_m:06}/{ano_atual}"  # Ex: 000034/25


        def gerar_id(cursor, tabela, campo):
            try:
                sql = f"""
                DECLARE @ret INT;
                EXEC @ret = hsGerarCodigo '{tabela}', '{campo}';
                SELECT @ret AS id;
                """
                cursor.execute(sql)

                # Avança até o result set do SELECT @ret
                while True:
                    try:
                        row = cursor.fetchone()
                        if row is not None:
                            return row[0]
                    except pyodbc.ProgrammingError:
                        pass  # não é um SELECT ainda
                    if not cursor.nextset():
                        break

                raise Exception(f"Nenhum resultado retornado por hsGerarCodigo para {tabela}.{campo}")
            except Exception as e:
                print(f"Erro ao gerar ID para {tabela}.{campo}: {e}")
                raise



        # Geração segura dos IDs via hsGerarCodigo
        id_house = gerar_id(cursor_sql, 'mov_Logistica_House', 'IdLogistica_House')
        id_master = gerar_id(cursor_sql, 'mov_Logistica_Master', 'IdLogistica_Master')
        id_viagem = gerar_id(cursor_sql, 'mov_Logistica_Viagem', 'IdLogistica_Viagem')
        id_moeda = gerar_id(cursor_sql, 'mov_Logistica_Moeda', 'IdLogistica_Moeda')
        id_equipamento = gerar_id(cursor_sql, 'mov_Logistica_Maritima_Equipamento', 'IdLogistica_Maritima_Equipament')
        id_projeto_atividade = gerar_id(cursor_sql, 'mov_Projeto_Atividade', 'IdProjeto_Atividade')
        id_campo_livre = gerar_id(cursor_sql, 'mov_Campo_Livre', 'IdCampo_Livre')


        dados_registro = f'<Registro><Origem><Classe Nome="TLogisticaHouse"><PrimaryKey Nome="IdLogistica_House" Tipo="3">{id_house}</PrimaryKey></Classe></Origem></Registro>'


        # Inserts
        cursor_sql.execute("""
            INSERT INTO mov_Projeto_Atividade (
                IdProjeto_Atividade, Dados_Registro, Referencia, IdProjeto_Tarefa, IdEmpresa_Sistema
            ) VALUES (?, ?, ?, ?, ?)
        """, (id_projeto_atividade, dados_registro, numero_processo, id_tarefa, 1))

        cursor_sql.execute("""
            INSERT INTO mov_Logistica_Master (
                IdLogistica_Master, IdCompanhia_Transporte, IdOrigem, IdDestino,
                IdMoeda_Processo, Modalidade_Pagamento, Modalidade_Processo,
                Tipo_Operacao, Data_Processo, Numero_Processo,
                IdEmpresa_Sistema, Situacao_Embarque, Situacao_Tracking_Viagens, Master_Direto
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            id_master, dados_dict['shipowner_id'], dados_dict['origin_id'], dados_dict['destination_id'],
            dados_dict['coin_id'], 2, dados_dict['modality_id'],
            dados_dict['operation_id'], datetime.now(), numero_processo_m, 1, 5, 0, 0
        ))

        cursor_sql.execute("""
            INSERT INTO mov_Logistica_House (
                IdLogistica_House, IdLogistica_Master, IdCliente, IdExportador, IdImportador, IdEmpresa_Sistema,
                Numero_Processo, Situacao_Agenciamento, Situacao_Pagamento,
                Situacao_Acerto_Agente, Tipo_Carga, Tipo_Projeto, Trecho,
                Data_Abertura_Processo, Agenciamento_Carga, Desembaraco_Aduaneiro,
                Demurrage_Detention, Consolidado_House, Ativo, IdIncoterm, IdTipo_Processo,
                Atualizacao_Taxas_Comercial, Carga_Perigosa, Comissao_Coleta, Comissao_Entrega,
                IdFuncionario_Criacao, Modalidade_Coleta, Modalidade_Entrega, Modalidade_Pagamento,
                Percentual_Seguro, Seguro, Situacao_Recebimento, Situacao_Siscoserv, Tipo_Servico,
                IdProjeto_Atividade 
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            id_house, id_master, dados_dict['client_id'], dados_dict['shipper_id'], dados_dict['consignee_id'], 1,
            numero_processo, 1, 0, 0, dados_dict['cargo_type_id'], 1, 0,
            datetime.now(), 1, 0, 0, 0, 1, dados_dict['incoterm_id'], 15,
            1, 0, 0, 0, 64698, 0, 0, 2,
            0.00, 0, 0, 1, 4, id_projeto_atividade
        ))

        cursor_sql.execute("""
            INSERT INTO mov_Logistica_Maritima_Master (
                IdLogistica_Master, Forma_Controle_Rota
            ) VALUES (?, ?)
        """, (id_master, None))

        cursor_sql.execute("""
            INSERT INTO mov_Logistica_Viagem (
                IdLogistica_Viagem, IdLogistica_House, IdOrigem, IdDestino,
                IdCompanhia_Transporte, Modalidade_Transporte, Tipo_Viagem,
                Situacao_Embarque, Programado, Sequencia, Manter_Vinculo_Manual, 
                Manter_Data_Embarque, Manter_Data_Desembarque, Manter_Previsao_Embarque,
                Manter_Previsao_Desembarque, Manter_Navio, Manter_Viagem, Manter_Modalidade,
                Manter_Tipo_Viagem, Ignorar_Tracking, Precedencia, Tipo_Servico, Ultima_Viagem, Ultimo_Transbordo_Escala
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            id_viagem, id_house, dados_dict['origin_id'], dados_dict['destination_id'],
            dados_dict['shipowner_id'], dados_dict['modality_id'], 4,
            0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0
        ))

        cursor_sql.execute("""
            INSERT INTO mov_Logistica_Moeda (
                IdLogistica_Moeda, IdLogistica_House, IdMoeda
            ) VALUES (?, ?, ?)
        """, (id_moeda, id_house, dados_dict['coin_id']))

        # Verifica se o equipamento foi informado
        if dados_dict.get('equip_id'):
            cursor_sql.execute("""
                INSERT INTO mov_Logistica_Maritima_Equipamento (
                    IdLogistica_Maritima_Equipament, IdLogistica_House, IdEquipamento_Maritimo, Quantidade
                ) VALUES (?, ?, ?, ?)
            """, (id_equipamento, id_house, dados_dict['equip_id'], 1))
        else:
            print(f"Campo Equipamento em branco para o processo {dados_dict.get('numero_processo')}, pulando insert.")


        cursor_sql.execute("""
            INSERT INTO mov_Logistica_Maritima_House (
                IdLogistica_House
            ) VALUES (?)
        """, (id_house,))

# Garantir que o valor seja numérico ou None
        try:
            temperatura = float(dados_dict.get("temperature")) if dados_dict.get("temperature") is not None else None
        except Exception:
            temperatura = None

        cursor_sql.execute("""
            INSERT INTO mov_Campo_Livre (
                IdCampo_Livre,
                IdConfiguracao_Campo_Livre,
                Valor_Real4
            ) VALUES (?, ?, ?)
        """, (
            id_campo_livre,
            32,
            temperatura  # já tratado acima com dados_dict
        ))

        # Inserir na mov_Logistica_Campo_Livre
        cursor_sql.execute("""
            INSERT INTO mov_Logistica_Campo_Livre (
                IdCampo_Livre,
                IdLogistica_House
            ) VALUES (?, ?)
        """, (
            id_campo_livre,
            id_house  # já gerado anteriormente
        ))

        # Atualizar processed_hs
        cursor_pg.execute("""
            UPDATE freehand SET processed_hs = TRUE WHERE id = %s
        """, (dados_dict["id"],))
        POSTGRES_CONN.commit()
        conn_sql.commit()

        print(f"Dados inseridos no SQL Server com sucesso! Processo: {numero_processo}")
    
if __name__ == "__main__":
    inserir_sqlserver()
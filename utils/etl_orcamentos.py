# Importar bibliotecas
import requests
import base64
import time
import json
import os
import pandas as pd
from sqlalchemy import create_engine

def etl_orcamentos(subdominio, usuario_api, senha_api, building_ids, db_url):
    # Dados da API Sienge
    url_api_base = f"https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/building-cost-estimation-items"

    # Autenticação
    credenciais = base64.b64encode(f"{usuario_api}:{senha_api}".encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': f'Basic {credenciais}',
        'Content-Type': 'application/json'
    }
    
    # Fazer a requisição GET na API com tentativas de reconexão
    max_retries=3
    retry_delay=60
    for attempt in range(max_retries):
        try:
            response = requests.get(url_api_base, headers=headers, timeout=600)  # Timeout de 10 minutos
            response.raise_for_status()  # Levanta um erro para status codes 4xx/5xx
            print("Requisição bem-sucedida!")
            break  # Sair do loop se a requisição for bem-sucedida

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            if attempt < max_retries - 1:
                print(f"Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                print("Número máximo de tentativas excedido.")
                return  # Sair se todas as tentativas falharem

    # Se a requisição for bem-sucedida, processar os dados
    data = response.json().get('data')
    df = pd.DataFrame(data)

    # Remover colunas específicas no DataFrame pandas
    colunas_remover = ["projects", "pricesByCategory", "scheduledPercentComplete", "percentComplete", "measuredQuantity"]
    df_colunas_removidas = df.drop(columns=colunas_remover)

    # Explodir a coluna `tasks` no DataFrame pandas
    df_tasks_exploded = df_colunas_removidas.explode('tasks')

    # Verificar se a coluna `tasks` existe e contém dados
    if 'tasks' in df_tasks_exploded.columns:
        df_tasks_exploded['presentationId'] = df_tasks_exploded['tasks'].apply(lambda x: x['presentationId'] if pd.notnull(x) else None)
    else:
        df_tasks_exploded['presentationId'] = None

    # Preencher os valores `None` em `presentationId` com uma string vazia
    df_tasks_exploded['presentationId'] = df_tasks_exploded['presentationId'].fillna('')

    # Manter todas as colunas exceto `tasks`
    df_final_pandas = df_tasks_exploded.drop(columns=['tasks'])

    # Filtrar obras
    building_id = building_ids
    filtered_df = df_final_pandas[df_final_pandas['buildingId'].isin(building_id)]

    # Função para determinar o nível com base na contagem de caracteres
    def determine_wbs_level(wbsCode):
        length = len(wbsCode)
        if length == 2:
            return 'Nível 1'
        elif length == 6:
            return 'Nível 2'
        elif length == 10:
            return 'Nível 3'
        elif length == 14:
            return 'Nível 4'
        else:
            return 'Nível Desconhecido'

    # Aplicando a função para criar a nova coluna 'wbs'
    filtered_df['wbs'] = filtered_df['wbsCode'].apply(determine_wbs_level)

    dtype_dict = {
        'buildingId': 'str', 
        'buildingName': 'str', 
        'buildingStatus': 'str', 
        'versionNumber': 'str',
        'buildingUnitId': 'str', 
        'buildingUnitName': 'str', 
        'id': 'str', 
        'wbsCode': 'str', 
        'workItemId': 'str',
        'description': 'str', 
        'unitOfMeasure': 'str', 
        'quantity': 'float64', 
        'unitPrice': 'float64', 
        'totalPrice': 'float64',
        'baseTotalPrice': 'float64', 
        'presentationId': 'str',
        'wbs': 'str'
    }

    # Aplicar os tipos de dados ao DataFrame
    df_final = filtered_df.astype(dtype_dict)

    # Configuração da conexão com o banco de dados PostgreSQL
    engine = create_engine(db_url)

    # Salvar o DataFrame na tabela 'orcamentos' do banco de dados
    df_final.to_sql(name='orcamentos', con=engine, if_exists='replace', index=False)
    print("Dados de Orçamentos salvos no banco de dados PostgreSQL.")
    pass
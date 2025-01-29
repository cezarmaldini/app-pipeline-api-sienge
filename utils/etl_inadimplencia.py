# Importar Bibliotecas
import requests
import base64
import time
from datetime import datetime
import json
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def etl_inadimplencia(subdominio, usuario_api, senha_api, company_ids, db_url):
    # Dados da API Sienge
    url_api_base = f"https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/defaulters-receivable-bills"

    # Autenticação
    credenciais = base64.b64encode(f"{usuario_api}:{senha_api}".encode('utf-8')).decode('utf-8')
    
    headers = {
        'Authorization': f'Basic {credenciais}',
        'Content-Type': 'application/json'
    }

    # Lista para armazenar todos os dados
    all_data = []

    # Loop sobre todos os 'company_ids'
    max_retries = 3
    retry_delay = 60

    for company_id in company_ids:
        params = {
            'companyId': company_id,
        }

        # Fazer a requisição GET na API com tentativas de reconexão
        for attempt in range(max_retries):
            try:
                response = requests.get(url_api_base, params=params, headers=headers, timeout=900)  # Timeout de 15 minutos
                response.raise_for_status()  # Levanta um erro para status codes 4xx/5xx
                print(f"Requisição bem-sucedida para company_id {company_id}!")
                break  # Sair do loop se a requisição for bem-sucedida

            except requests.exceptions.RequestException as e:
                print(f"Erro na requisição para company_id {company_id}: {e}")
                if attempt < max_retries - 1:
                    print(f"Tentando novamente em {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    print(f"Número máximo de tentativas excedido para company_id {company_id}.")
                    return  # Sair se todas as tentativas falharem

        # Se a requisição for bem-sucedida, processar os dados
        data = response.json().get('data')
        if data:
            all_data.extend(data)
    
    df = pd.DataFrame(all_data)

    df = df.explode('defaulterInstallments')

    if 'defaulterInstallments' in df.columns:
        df['installmentId'] = df['defaulterInstallments'].apply(lambda x: x['installmentId'] if pd.notnull(x) and 'installmentId' in x else None)
        df['correctedValueWithAdditions'] = df['defaulterInstallments'].apply(lambda x: x['correctedValueWithAdditions'] if pd.notnull(x) and 'correctedValueWithAdditions' in x else None)
    else:
        df['installmentId'] = None
        df['correctedValueWithAdditions'] = None

    df['installmentId'] = df['installmentId'].fillna('')
    df['correctedValueWithAdditions'] = df['correctedValueWithAdditions'].fillna('')

    columns_remove = [
        'issueDate', 'documentNumber', 'units', 'defaulterInstallments'
    ]

    df = df.drop(columns=columns_remove)

    # Configuração da conexão com o banco de dados PostgreSQL
    engine = create_engine(db_url)

    # Salvar o DataFrame na tabela 'inadimplencia' do banco de dados
    df.to_sql(name='inadimplencia', con=engine, if_exists='replace', index=False)
    print("Dados de Inadimplencia salvos no banco de dados PostgreSQL.")
    pass
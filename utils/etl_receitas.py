import requests
import base64
import time
from datetime import datetime
import json
import os
import pandas as pd
from sqlalchemy import create_engine

def etl_receitas(subdominio, usuario_api, senha_api, company_ids, db_url):
    # Dados da API Sienge
    url_api_base = f"https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/income"
    start_date = '2010-01-01'
    selection_type = 'I'
    
    # Autenticação
    credenciais = base64.b64encode(f"{usuario_api}:{senha_api}".encode('utf-8')).decode('utf-8')

    # Parâmetros da consulta
    end_date = datetime.now().strftime('%Y-%m-%d')
    params = {
        'startDate': start_date,
        'endDate': end_date,
        'selectionType': selection_type
    }

    headers = {
        'Authorization': f'Basic {credenciais}',
        'Content-Type': 'application/json'
    }

    # Fazer a requisição GET na API com tentativas de reconexão
    max_retries=3
    retry_delay=60
    for attempt in range(max_retries):
        try:
            response = requests.get(url_api_base, params=params, headers=headers, timeout=900)  # Timeout de 15 minutos
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

    #DataFrame colunas Receipts
    df_receipts = df[['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId', 'receipts']]

    df_expanded = df_receipts.explode('receipts')

    # Expandir o conteúdo da coluna 'receipts'
    df_receipts_expanded = pd.json_normalize(df_expanded['receipts'])

    # Resetar os índices de ambos os DataFrames
    df_expanded.reset_index(drop=True, inplace=True)
    df_receipts_expanded.reset_index(drop=True, inplace=True)

    # Juntar as colunas expandidas de 'receipts' com o DataFrame original
    df_final = pd.concat([df_expanded.drop(columns=['receipts']), df_receipts_expanded], axis=1)

    # Remove as linhas onde 'operationTypeId' é NaN
    df_cleaned = df_final.dropna(subset=['operationTypeId'])

    colunas_remover = [
        "calculationDate", "accountCompanyId", "accountNumber", "accountType", "sequencialNumber",  "indexerId", "embeddedInterestAmount", "proRata", "bankMovements"
    ]

    df_final_receipts = df_cleaned.drop(columns=colunas_remover)

    #DataFrame Receitas Original
    remove_columns = [
        "businessAreaId", "businessAreaName", "groupCompanyId", "groupCompanyName",
        "holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "businessTypeId",
        "businessTypeName", "issueDate", "billDate", "installmentBaseDate", "periodicityType",
        "embeddedInterestAmount", "interestType", "interestRate", "correctionType", "interestBaseDate",
        "defaulterSituation", "subJudicie", "paymentTerm", "indexerId", "embeddedInterestAmount",
        "indexerName", "receipts"
        ]

    df_columns = df.drop(columns=remove_columns)

    df_categorias = df_columns.explode('receiptsCategories')

    if 'receiptsCategories' in df.columns:
        df_categorias['costCenterId'] = df_categorias['receiptsCategories'].apply(lambda x: x['costCenterId'] if pd.notnull(x) and 'costCenterId' in x else None)
        df_categorias['costCenterName'] = df_categorias['receiptsCategories'].apply(lambda x: x['costCenterName'] if pd.notnull(x) and 'costCenterName' in x else None)
        df_categorias['financialCategoryId'] = df_categorias['receiptsCategories'].apply(lambda x: x['financialCategoryId'] if pd.notnull(x) and 'financialCategoryId' in x else None)
        df_categorias['financialCategoryName'] = df_categorias['receiptsCategories'].apply(lambda x: x['financialCategoryName'] if pd.notnull(x) and 'financialCategoryName' in x else None)
        df_categorias['financialCategoryRate'] = df_categorias['receiptsCategories'].apply(lambda x: x['financialCategoryRate'] if pd.notnull(x) and 'financialCategoryRate' in x else None)
    else:
        df_categorias['costCenterId'] = None
        df_categorias['costCenterName'] = None
        df_categorias['financialCategoryId'] = None
        df_categorias['financialCategoryName'] = None
        df_categorias['financialCategoryRate'] = None

    df_categorias['costCenterId'] = df_categorias['costCenterId'].fillna('')
    df_categorias['costCenterName'] = df_categorias['costCenterName'].fillna('')
    df_categorias['financialCategoryId'] = df_categorias['financialCategoryId'].fillna('')
    df_categorias['financialCategoryName'] = df_categorias['financialCategoryName'].fillna('')
    df_categorias['financialCategoryRate'] = df_categorias['financialCategoryRate'].fillna('')

    # Confirmar se todas as colunas para a junção existem em ambos os DataFrames
    common_columns = ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId']
    for col in common_columns:
        if col not in df_categorias.columns:
            print(f"Coluna {col} não encontrada no df_categorias")
        if col not in df_final_receipts.columns:
            print(f"Coluna {col} não encontrada no df_final_receipts")

    # Realizar a junção (merge) dos dois DataFrames
    df_resultado = pd.merge(
        df_categorias, # DataFrame original,
        df_final_receipts[[
            'companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId',
            'operationTypeId', 'operationTypeName', 'grossAmount', 'monetaryCorrectionAmount',
            'interestAmount', 'fineAmount', 'discountAmount', 'taxAmount', 'netAmount',
            'additionAmount', 'insuranceAmount', 'dueAdmAmount', 'paymentDate'
        ]], # DataFrame com as colunas que desejamos levar para o DataFrame orginal
        on= ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId'], # Chaves comuns
        how='left'  # Mantemos todos os dados do df_categorias, mesmo que não haja correspondência no df_final_receipts
    )

    print("Colunas mescladas com sucesso!!!")

    # Filtrar empresas
    company_id = company_ids # Filtrar todas os projetos que deseja retornar os dados
    df_company = df_resultado[df_resultado['companyId'].isin(company_id)]

    # Converter colunas específicas para int ou float, lidando com valores nulos
    float_columns = ['originalAmount', 'discountAmount_x', 'taxAmount_x', 'balanceAmount', 
                    'correctedBalanceAmount', 'grossAmount', 'monetaryCorrectionAmount', 
                    'interestAmount', 'fineAmount', 'discountAmount_y', 'taxAmount_y', 
                    'netAmount', 'additionAmount', 'insuranceAmount', 'dueAdmAmount', 
                    'financialCategoryRate']

    df_company[float_columns] = df_company[float_columns].apply(pd.to_numeric, errors='coerce')

    # Dicionário com os tipos de dados desejados para cada coluna
    dtype_dict = {
        'companyId': 'str',
        'companyName': 'str',
        'projectId': 'str',
        'projectName': 'str',
        'clientId': 'str',
        'clientName': 'str',
        'billId': 'str',
        'installmentId': 'str',
        'documentIdentificationId': 'str',
        'documentIdentificationName': 'str',
        'documentNumber': 'str',
        'documentForecast': 'str',
        'originId': 'str',
        'originalAmount': 'float64',
        'discountAmount_x': 'float64',
        'taxAmount_x': 'float64',
        'dueDate': 'datetime64[ns]',
        'balanceAmount': 'float64',
        'correctedBalanceAmount': 'float64',
        'mainUnit': 'str',
        'installmentNumber': 'str',
        'receiptsCategories': 'str',
        'operationTypeId': 'str',
        'operationTypeName': 'str',
        'grossAmount': 'float64',
        'monetaryCorrectionAmount': 'float64',
        'interestAmount': 'float64',
        'fineAmount': 'float64',
        'discountAmount_y': 'float64',
        'taxAmount_y': 'float64',
        'netAmount': 'float64',
        'additionAmount': 'float64',
        'insuranceAmount': 'float64',
        'dueAdmAmount': 'float64',
        'paymentDate': 'datetime64[ns]',
        'costCenterId': 'str',
        'costCenterName': 'str',
        'financialCategoryId': 'str',
        'financialCategoryName': 'str',
        'financialCategoryRate': 'float64',
    }

    # Aplicar os tipos de dados ao DataFrame
    df_final = df_company.astype(dtype_dict)
    df_final= df_final.drop(columns=['receiptsCategories'])
    df_final['financialCategoryRate'] = df_final['financialCategoryRate']/100
    
    # Configuração da conexão com o banco de dados PostgreSQL
    engine = create_engine(db_url)

    # Salvar o DataFrame na tabela 'receitas' do banco de dados
    df_final.to_sql(name='receitas', con=engine, if_exists='replace', index=False)

    print("Dados de Receitas salvos no banco de dados PostgreSQL.")
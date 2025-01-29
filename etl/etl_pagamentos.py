# Importando Bibliotecas
import requests
import base64
import time
from datetime import datetime
import json
import os
import pandas as pd
from sqlalchemy import create_engine

def etl_pagamentos(subdominio, usuario_api, senha_api, project_ids, company_ids, db_url):
    # Dados da API Sienge
    url_api_base = f"https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/outcome"
    start_date = '2010-01-01'
    selection_type = 'I'
    correction_Indexer = '1'

    # Autenticação
    credenciais = base64.b64encode(f"{usuario_api}:{senha_api}".encode('utf-8')).decode('utf-8')

    # Parâmetros da consulta
    end_date = datetime.now().strftime('%Y-%m-%d')
    params = {
        'startDate': start_date,
        'endDate': end_date,
        'selectionType': selection_type,
        'correctionIndexerId': correction_Indexer,
        'correctionDate': end_date
    }

    headers = {
        'Authorization': f'Basic {credenciais}',
        'Content-Type': 'application/json'
    }

    data = None  # Inicializar a variável para armazenar os dados

    # Fazer a requisição GET na API com tentativas de reconexão
    max_retries=3
    retry_delay=60
    for attempt in range(max_retries):
        try:
            print(f"Tentativa {attempt + 1} de {max_retries}...")
            response = requests.get(url_api_base, params=params, headers=headers, timeout=1800)  # Timeout de 30 minutos
            response.raise_for_status()  # Levanta um erro para status codes 4xx/5xx
            print("Requisição bem-sucedida!")
            data = response.json().get('data')  # Armazena os dados da API
            break  # Sai do loop após sucesso

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            if attempt < max_retries - 1:
                print(f"Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                print("Número máximo de tentativas excedido.")
                return  # Encerra a função se todas as tentativas falharem

    if data is None:
        print("Nenhum dado foi obtido da API.")
        return

    # Continuar com o processamento dos dados
    print("Iniciando o processamento dos dados...")

    try:
        df = pd.DataFrame(data)
        print("Dados convertidos para DataFrame.")

        # Filtrar projetos
        project_id = project_ids  # Filtrar todas os projetos que deseja retornar os dados
        df = df[df['projectId'].isin(project_id)]
        print("Projetos filtrados.")

        # Filtrar empresas
        company_id = company_ids  # Filtrar todas as empresas que deseja retornar os dados
        df = df[df['companyId'].isin(company_id)]
        print("Empresas filtradas.")

        # DataFrame Colunas Payments
        df_payments = df[['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId', 'payments']]
        print("DataFrame de pagamentos criado.")

        df_expanded = df_payments.explode('payments')
        df_expanded_payments = pd.json_normalize(df_expanded['payments'])
        df_expanded.reset_index(drop=True, inplace=True)
        df_expanded_payments.reset_index(drop=True, inplace=True)
        df_final_payments = pd.concat([df_expanded.drop(columns=['payments']), df_expanded_payments], axis=1)
        df_cleaned = df_final_payments.dropna(subset=['operationTypeId'])

        colunas_remover = [
            "calculationDate", "paymentAuthentication", "sequencialNumber", "correctedNetAmount", "bankMovements"
        ]
        df_final_payments = df_cleaned.drop(columns=colunas_remover)
        print("DataFrame de pagamentos limpo.")

        # DataFrame Pagamentos Original
        columns_remove = ['businessAreaId', 'businessAreaName', 'groupCompanyId', 'groupCompanyName',
                          'holdingId', 'holdingName', 'subsidiaryId', 'subsidiaryName', 'businessTypeId', 'businessTypeName',
                          'indexerId', 'indexerName', 'issueDate', 'installmentBaseDate', 'authorizationStatus', 'billDate',
                          'registeredUserId', 'registeredBy', 'registeredDate', 'departamentsCosts',
                          'buildingsCosts', 'payments', 'authorizations']
        df_columns = df.drop(columns=columns_remove)
        print("Colunas removidas do DataFrame original.")

        # Explodir a coluna `paymentsCategories` no DataFrame pandas
        df_expandir_categorias = df_columns.explode('paymentsCategories')

        # Verificar se a coluna `paymentsCategories` existe e contém dados
        if 'paymentsCategories' in df_expandir_categorias.columns:
            df_expandir_categorias['costCenterId'] = df_expandir_categorias['paymentsCategories'].apply(
                lambda x: x['costCenterId'] if pd.notnull(x) and 'costCenterId' in x else None)
            df_expandir_categorias['costCenterName'] = df_expandir_categorias['paymentsCategories'].apply(
                lambda x: x['costCenterName'] if pd.notnull(x) and 'costCenterName' in x else None)
            df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['paymentsCategories'].apply(
                lambda x: x['financialCategoryId'] if pd.notnull(x) and 'financialCategoryId' in x else None)
            df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['paymentsCategories'].apply(
                lambda x: x['financialCategoryName'] if pd.notnull(x) and 'financialCategoryName' in x else None)
            df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['paymentsCategories'].apply(
                lambda x: x['financialCategoryRate'] if pd.notnull(x) and 'financialCategoryRate' in x else None)
        else:
            df_expandir_categorias['costCenterId'] = None
            df_expandir_categorias['costCenterName'] = None
            df_expandir_categorias['financialCategoryId'] = None
            df_expandir_categorias['financialCategoryName'] = None
            df_expandir_categorias['financialCategoryRate'] = None

        # Preencher os valores `None` com uma string vazia
        df_expandir_categorias['costCenterId'] = df_expandir_categorias['costCenterId'].fillna('')
        df_expandir_categorias['costCenterName'] = df_expandir_categorias['costCenterName'].fillna('')
        df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['financialCategoryId'].fillna('')
        df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['financialCategoryName'].fillna('')
        df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['financialCategoryRate'].fillna('')

        # Confirmar se todas as colunas para a junção existem em ambos os DataFrames
        common_columns = ['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId']
        for col in common_columns:
            if col not in df_expandir_categorias.columns:
                print(f"Coluna {col} não encontrada no df_expandir_categorias")
            if col not in df_final_payments.columns:
                print(f"Coluna {col} não encontrada no df_final_payments")

        # Realizar a junção (merge) dos dois DataFrames
        df_resultado_pagamentos = pd.merge(
            df_expandir_categorias,  # DataFrame original,
            df_final_payments[[
                'companyId', 'projectId', 'billId', 'installmentId',
                'documentIdentificationId', 'operationTypeId', 'operationTypeName',
                'grossAmount', 'monetaryCorrectionAmount', 'interestAmount',
                'fineAmount', 'discountAmount', 'taxAmount', 'netAmount',
                'paymentDate'
            ]],  # DataFrame com as colunas que desejamos levar para o DataFrame original
            on=['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId'],  # Chaves comuns
            how='left'  # Mantemos todos os dados do df_expandir_categorias, mesmo que não haja correspondência no df_final_payments
        )
        print("Colunas mescladas com sucesso para pagamentos!")

        # Dicionário com os tipos de dados desejados para cada coluna
        dtype_dict_pagamentos = {
            'companyId': 'int64',
            'companyName': 'str',
            'projectId': 'int64',
            'projectName': 'str',
            'creditorId': 'str',
            'creditorName': 'str',
            'billId': 'str',
            'installmentId': 'str',
            'documentIdentificationId': 'str',
            'documentIdentificationName': 'str',
            'documentNumber': 'str',
            'forecastDocument': 'str',
            'consistencyStatus': 'str',
            'originId': 'str',
            'originalAmount': 'float64',
            'discountAmount_x': 'float64',
            'taxAmount_x': 'float64',
            'dueDate': 'datetime64[ns]',
            'balanceAmount': 'float64',
            'correctedBalanceAmount': 'float64',
            'paymentsCategories': 'str',
            'operationTypeId': 'str',
            'operationTypeName': 'str',
            'grossAmount': 'float64',
            'monetaryCorrectionAmount': 'float64',
            'interestAmount': 'float64',
            'fineAmount': 'float64',
            'discountAmount_y': 'float64',
            'taxAmount_y': 'float64',
            'netAmount': 'float64',
            'paymentDate': 'datetime64[ns]',
            'costCenterId': 'int64',
            'costCenterName': 'str',
            'financialCategoryId': 'int64',
            'financialCategoryName': 'str',
            'financialCategoryRate': 'float64'
        }

        # Aplicar os tipos de dados ao DataFrame
        df_final_pagamentos = df_resultado_pagamentos.astype(dtype_dict_pagamentos)
        df_final_pagamentos = df_final_pagamentos.drop(columns=['paymentsCategories'])
        df_final_pagamentos['financialCategoryRate'] = df_final_pagamentos['financialCategoryRate'] / 100
        print("Tipos de dados aplicados ao DataFrame de pagamentos.")

        # DataFrame Apropriações
        remove_columns_aprop = [
            "businessAreaId", "businessAreaName", "groupCompanyId", "groupCompanyName",
            "holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "businessTypeId",
            "businessTypeName", "indexerId", "indexerName", "issueDate", "installmentBaseDate",
            "authorizationStatus", "billDate", "registeredUserId", "registeredBy", "registeredDate",
            "departamentsCosts", "authorizations", "paymentsCategories", "payments"
        ]
        df_remove_aprop = df.drop(columns=remove_columns_aprop)
        print("Colunas removidas do DataFrame para apropriações.")

        # Explodir a coluna `buildingsCosts` no DataFrame pandas
        df_expandir_aprop = df_remove_aprop.explode('buildingsCosts')

        # Verificar se a coluna `buildingsCosts` existe e contém dados
        if 'buildingsCosts' in df_expandir_aprop.columns:
            df_expandir_aprop['buildingId'] = df_expandir_aprop['buildingsCosts'].apply(
                lambda x: x['buildingId'] if pd.notnull(x) and 'buildingId' in x else None)
            df_expandir_aprop['buildingName'] = df_expandir_aprop['buildingsCosts'].apply(
                lambda x: x['buildingName'] if pd.notnull(x) and 'buildingName' in x else None)
            df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buildingsCosts'].apply(
                lambda x: x['buildingUnitId'] if pd.notnull(x) and 'buildingUnitId' in x else None)
            df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['buildingsCosts'].apply(
                lambda x: x['costEstimationSheetId'] if pd.notnull(x) and 'costEstimationSheetId' in x else None)
            df_expandir_aprop['rate'] = df_expandir_aprop['buildingsCosts'].apply(
                lambda x: x['rate'] if pd.notnull(x) and 'rate' in x else None)
        else:
            df_expandir_aprop['buildingId'] = None
            df_expandir_aprop['buildingName'] = None
            df_expandir_aprop['buildingUnitId'] = None
            df_expandir_aprop['costEstimationSheetId'] = None
            df_expandir_aprop['rate'] = None

        # Preencher os valores `None` com uma string vazia
        df_expandir_aprop['buildingId'] = df_expandir_aprop['buildingId'].fillna('')
        df_expandir_aprop['buildingName'] = df_expandir_aprop['buildingName'].fillna('')
        df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buildingUnitId'].fillna('')
        df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['costEstimationSheetId'].fillna('')
        df_expandir_aprop['rate'] = df_expandir_aprop['rate'].fillna(0)  # Assumindo que 0 é um valor apropriado

        # Confirmar se todas as colunas para a junção existem em ambos os DataFrames
        for col in common_columns:
            if col not in df_expandir_aprop.columns:
                print(f"Coluna {col} não encontrada no df_expandir_aprop")
            if col not in df_final_payments.columns:
                print(f"Coluna {col} não encontrada no df_final_payments")

        # Realizar a junção (merge) dos dois DataFrames
        df_resultado_aprop = pd.merge(
            df_expandir_aprop,  # DataFrame original,
            df_final_payments[[
                'companyId', 'projectId', 'billId', 'installmentId',
                'documentIdentificationId', 'operationTypeId', 'operationTypeName',
                'grossAmount', 'monetaryCorrectionAmount', 'interestAmount',
                'fineAmount', 'discountAmount', 'taxAmount', 'netAmount',
                'paymentDate'
            ]],  # DataFrame com as colunas que desejamos levar para o DataFrame original
            on=['companyId', 'projectId', 'billId', 'installmentId', 'documentIdentificationId'],  # Chaves comuns
            how='left'  # Mantemos todos os dados do df_expandir_aprop, mesmo que não haja correspondência no df_final_payments
        )
        print("Colunas mescladas com sucesso para apropriações!")

        # Dicionário com os tipos de dados desejados para cada coluna
        dtype_dict_aprop = {
            'companyId': 'int64',
            'companyName': 'str',
            'projectId': 'int64',
            'projectName': 'str',
            'creditorId': 'int64',
            'creditorName': 'str',
            'billId': 'int64',
            'installmentId': 'int64',
            'documentIdentificationId': 'str',
            'documentIdentificationName': 'str',
            'documentNumber': 'str',
            'forecastDocument': 'str',
            'consistencyStatus': 'str',
            'originId': 'str',
            'originalAmount': 'float64',
            'discountAmount_x': 'float64',
            'taxAmount_x': 'float64',
            'dueDate': 'datetime64[ns]',
            'balanceAmount': 'float64',
            'correctedBalanceAmount': 'float64',
            'buildingsCosts': 'str',
            'operationTypeId': 'str',
            'operationTypeName': 'str',
            'grossAmount': 'float64',
            'monetaryCorrectionAmount': 'float64',
            'interestAmount': 'float64',
            'fineAmount': 'float64',
            'discountAmount_y': 'float64',
            'taxAmount_y': 'float64',
            'netAmount': 'float64',
            'paymentDate': 'datetime64[ns]',
            'buildingId': 'str',
            'buildingName': 'str',
            'buildingUnitId': 'str',
            'costEstimationSheetId': 'str',
            'rate': 'float64'
        }

        # Aplicar os tipos de dados ao DataFrame
        df_final_aprop = df_resultado_aprop.astype(dtype_dict_aprop)
        df_final_aprop = df_final_aprop.drop(columns=['buildingsCosts'])
        df_final_aprop['rate'] = df_final_aprop['rate'] / 100
        print("Tipos de dados aplicados ao DataFrame de apropriações.")

        engine = create_engine(db_url)
        print("Conexão com o banco de dados estabelecida.")

        # Salvar o DataFrame na tabela 'pagamentos' do banco de dados
        df_final_pagamentos.to_sql(name='pagamentos', con=engine, if_exists='replace', index=False)
        print("Dados de Pagamentos salvos no banco de dados PostgreSQL.")

        # Salvar o DataFrame na tabela 'apropriacoes' do banco de dados
        df_final_aprop.to_sql(name='apropriacoes', con=engine, if_exists='replace', index=False)
        print("Dados de Apropriações salvos no banco de dados PostgreSQL.")

    except Exception as e:
        print(f"Ocorreu um erro durante o processamento dos dados: {e}")
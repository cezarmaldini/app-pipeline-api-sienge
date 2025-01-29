# Importar bibliotecas
import requests
import base64
import time
import json
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

def etl_mov_bancarias(subdominio, usuario_api, senha_api, company_ids, db_url):
    # Dados da API Sienge
    url_api_base = f"https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/bank-movement"

    # Autenticação
    credenciais = base64.b64encode(f"{usuario_api}:{senha_api}".encode('utf-8')).decode('utf-8')

    # Parâmetros da consulta
    start_date = '2010-01-01',
    end_date = datetime.now().strftime('%Y-%m-%d')
    params = {
        'startDate': start_date,
        'endDate': end_date
    }

    headers = {
        'Authorization': f'Basic {credenciais}',
        'Content-Type': 'application/json'
    }

    data = None  # Inicializar a variável para armazenar os dados
    max_retries=3
    retry_delay=300
    # Fazer a requisição GET na API com tentativas de reconexão
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

        # Processo para Armazenar a Tabela de Movimentações Bancárias no Lakehouse!!!

        # Remover colunas específicas no DataFrame pandas
        colunas_remover = ["holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "departamentCosts", "buldingCosts"]
        df_colunas_removidas = df.drop(columns=colunas_remover, errors='ignore')
        print("Colunas específicas removidas do DataFrame.")

        # Explodir a coluna `financialCategories` no DataFrame pandas
        df_expandir_categorias = df_colunas_removidas.explode('financialCategories')
        print("Coluna 'financialCategories' explodida.")

        # Verificar se a coluna `financialCategories` existe e contém dados
        if 'financialCategories' in df_expandir_categorias.columns:
            df_expandir_categorias['costCenterId'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: str(x['costCenterId']) if isinstance(x, dict) and 'costCenterId' in x else None)
            df_expandir_categorias['costCenterName'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: x['costCenterName'] if isinstance(x, dict) and 'costCenterName' in x else None)
            df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: x['financialCategoryId'] if isinstance(x, dict) and 'financialCategoryId' in x else None)
            df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: x['financialCategoryName'] if isinstance(x, dict) and 'financialCategoryName' in x else None)
            df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: float(x['financialCategoryRate']) if isinstance(x, dict) and 'financialCategoryRate' in x else None)
            df_expandir_categorias['projectId'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: x['projectId'] if isinstance(x, dict) and 'projectId' in x else None)
            df_expandir_categorias['projectName'] = df_expandir_categorias['financialCategories'].apply(
                lambda x: x['projectName'] if isinstance(x, dict) and 'projectName' in x else None)
            print("Colunas extraídas de 'financialCategories'.")
        else:
            df_expandir_categorias['costCenterId'] = None
            df_expandir_categorias['costCenterName'] = None
            df_expandir_categorias['financialCategoryId'] = None
            df_expandir_categorias['financialCategoryName'] = None
            df_expandir_categorias['financialCategoryRate'] = None
            df_expandir_categorias['projectId'] = None
            df_expandir_categorias['projectName'] = None
            print("'financialCategories' não encontrada ou vazia. Colunas preenchidas com None.")

        # Preencher os valores `None` com valores padrão
        df_expandir_categorias['costCenterId'] = df_expandir_categorias['costCenterId'].fillna('')
        df_expandir_categorias['costCenterName'] = df_expandir_categorias['costCenterName'].fillna('')
        df_expandir_categorias['financialCategoryId'] = df_expandir_categorias['financialCategoryId'].fillna('')
        df_expandir_categorias['financialCategoryName'] = df_expandir_categorias['financialCategoryName'].fillna('')
        df_expandir_categorias['financialCategoryRate'] = df_expandir_categorias['financialCategoryRate'].fillna(0.0)
        df_expandir_categorias['projectId'] = df_expandir_categorias['projectId'].fillna('')
        df_expandir_categorias['projectName'] = df_expandir_categorias['projectName'].fillna('')
        print("Valores 'None' preenchidos com valores padrão.")

        # Manter todas as colunas exceto `financialCategories`
        df_final_pandas = df_expandir_categorias.drop(columns=['financialCategories'], errors='ignore')
        print("Coluna 'financialCategories' removida do DataFrame final.")

        # Filtrar empresas
        company_id = company_ids  # Filtrar todas as empresas que deseja retornar os dados
        df_final_pandas = df_final_pandas[df_final_pandas['companyId'].isin(company_id)]
        print(f"Dados filtrados para as empresas: {company_ids}")

        # Processo para Armazenar a Tabela de Movimentações Bancárias com Apropriações no Lakehouse!!!

        # Remover colunas específicas no DataFrame pandas
        colunas_remover_aprop = ["holdingId", "holdingName", "subsidiaryId", "subsidiaryName", "departamentCosts"]
        df_colunas_removidas_aprop = df.drop(columns=colunas_remover_aprop, errors='ignore')
        print("Colunas específicas removidas para apropriações.")

        # Explodir a coluna `financialCategories` no DataFrame pandas
        df_expandir_aprop = df_colunas_removidas_aprop.explode('financialCategories')
        print("Coluna 'financialCategories' explodida para apropriações.")

        # Explodir a coluna `buldingCosts` no DataFrame pandas
        df_expandir_aprop = df_expandir_aprop.explode('buldingCosts')
        print("Coluna 'buldingCosts' explodida para apropriações.")

        # Verificar se a coluna `financialCategories` existe e contém dados
        if 'financialCategories' in df_expandir_aprop.columns:
            df_expandir_aprop['costCenterId'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['costCenterId'] if isinstance(x, dict) and 'costCenterId' in x else None)
            df_expandir_aprop['costCenterName'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['costCenterName'] if isinstance(x, dict) and 'costCenterName' in x else None)
            df_expandir_aprop['financialCategoryId'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['financialCategoryId'] if isinstance(x, dict) and 'financialCategoryId' in x else None)
            df_expandir_aprop['financialCategoryName'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['financialCategoryName'] if isinstance(x, dict) and 'financialCategoryName' in x else None)
            df_expandir_aprop['financialCategoryRate'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['financialCategoryRate'] if isinstance(x, dict) and 'financialCategoryRate' in x else None)
            df_expandir_aprop['projectId'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['projectId'] if isinstance(x, dict) and 'projectId' in x else None)
            df_expandir_aprop['projectName'] = df_expandir_aprop['financialCategories'].apply(
                lambda x: x['projectName'] if isinstance(x, dict) and 'projectName' in x else None)
            print("Colunas extraídas de 'financialCategories' para apropriações.")
        else:
            df_expandir_aprop['costCenterId'] = None
            df_expandir_aprop['costCenterName'] = None
            df_expandir_aprop['financialCategoryId'] = None
            df_expandir_aprop['financialCategoryName'] = None
            df_expandir_aprop['financialCategoryRate'] = None
            df_expandir_aprop['projectId'] = None
            df_expandir_aprop['projectName'] = None
            print("'financialCategories' não encontrada ou vazia para apropriações. Colunas preenchidas com None.")

        # Verificar se a coluna `buldingCosts` existe e contém dados
        if 'buldingCosts' in df_expandir_aprop.columns:
            df_expandir_aprop['buildingId'] = df_expandir_aprop['buldingCosts'].apply(
                lambda x: x['buildingId'] if isinstance(x, dict) and 'buildingId' in x else None)
            df_expandir_aprop['buildingName'] = df_expandir_aprop['buldingCosts'].apply(
                lambda x: x['buildingName'] if isinstance(x, dict) and 'buildingName' in x else None)
            df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buldingCosts'].apply(
                lambda x: x['buildingUnitId'] if isinstance(x, dict) and 'buildingUnitId' in x else None)
            df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['buldingCosts'].apply(
                lambda x: x['costEstimationSheetId'] if isinstance(x, dict) and 'costEstimationSheetId' in x else None)
            df_expandir_aprop['rate'] = df_expandir_aprop['buldingCosts'].apply(
                lambda x: x['rate'] if isinstance(x, dict) and 'rate' in x else None)
            print("Colunas extraídas de 'buldingCosts'.")
        else:
            df_expandir_aprop['buildingId'] = None
            df_expandir_aprop['buildingName'] = None
            df_expandir_aprop['buildingUnitId'] = None
            df_expandir_aprop['costEstimationSheetId'] = None
            df_expandir_aprop['rate'] = None
            print("'buldingCosts' não encontrada ou vazia. Colunas preenchidas com None.")

        # Preencher os valores `None` com valores padrão
        df_expandir_aprop['costCenterId'] = df_expandir_aprop['costCenterId'].fillna('')
        df_expandir_aprop['costCenterName'] = df_expandir_aprop['costCenterName'].fillna('')
        df_expandir_aprop['financialCategoryId'] = df_expandir_aprop['financialCategoryId'].fillna('')
        df_expandir_aprop['financialCategoryName'] = df_expandir_aprop['financialCategoryName'].fillna('')
        df_expandir_aprop['financialCategoryRate'] = df_expandir_aprop['financialCategoryRate'].fillna(0.0)
        df_expandir_aprop['projectId'] = df_expandir_aprop['projectId'].fillna('')
        df_expandir_aprop['projectName'] = df_expandir_aprop['projectName'].fillna('')
        df_expandir_aprop['buildingId'] = df_expandir_aprop['buildingId'].fillna('')
        df_expandir_aprop['buildingName'] = df_expandir_aprop['buildingName'].fillna('')
        df_expandir_aprop['buildingUnitId'] = df_expandir_aprop['buildingUnitId'].fillna('')
        df_expandir_aprop['costEstimationSheetId'] = df_expandir_aprop['costEstimationSheetId'].fillna('')
        df_expandir_aprop['rate'] = df_expandir_aprop['rate'].fillna(0.0)
        print("Valores 'None' preenchidos com valores padrão para apropriações.")

        # Manter todas as colunas exceto `financialCategories` e `buldingCosts`
        df_final_pandas_aprop = df_expandir_aprop.drop(columns=['financialCategories', 'buldingCosts'], errors='ignore')
        print("Colunas 'financialCategories' e 'buldingCosts' removidas do DataFrame final de apropriações.")

        # Filtrar empresas
        company_ids_aprop = company_ids  # Filtrar todas as empresas que deseja retornar os dados
        df_final_pandas_aprop = df_final_pandas_aprop[df_final_pandas_aprop['companyId'].isin(company_ids_aprop)]
        print(f"Dados de apropriações filtrados para as empresas: {company_ids_aprop}")

        engine = create_engine(db_url)
        print("Conexão com o banco de dados estabelecida.")

        # Salvar o DataFrame na tabela 'movimentacoes_bancarias' do banco de dados
        df_final_pandas.to_sql(name='movimentacoes_bancarias', con=engine, if_exists='replace', index=False)
        print("Dados de Movimentações Bancárias salvos no banco de dados PostgreSQL.")

        # Salvar o DataFrame na tabela 'apropriacoes_mov_bancarias' do banco de dados
        df_final_pandas_aprop.to_sql(name='apropriacoes_mov_bancarias', con=engine, if_exists='replace', index=False)
        print("Dados de Apropriações das Movimentações Bancárias salvos no banco de dados PostgreSQL.")

    except Exception as e:
        print(f"Ocorreu um erro durante o processamento dos dados: {e}")
    pass
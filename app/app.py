import streamlit as st
from utils.etl_inadimplencia import etl_inadimplencia
from utils.etl_insumos import etl_insumos
from utils.etl_mov_bancarias import etl_mov_bancarias
from utils.etl_orcamentos import etl_orcamentos
from utils.etl_pagamentos import etl_pagamentos
from utils.etl_receitas import etl_receitas
from datetime import datetime
import sys
import os

# Adicionar o diretório raiz ao PYTHONPATH para facilitar as importações
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Adicionar logotipo da empresa centralizado
st.markdown("""
    <div style="text-align: center;">
        <img src="https://i.ibb.co/gRZdQDG/TATICO-logotipo-02-colorido.png" width="400" />
    </div>
""", unsafe_allow_html=True)


def main():
    st.title("Processamento de Dados")

    st.write("""    
    Esta aplicação executa um processo de Extração dos dados no ERP, faz o Tratamento desses dados e na sequência salva em um Banco de Dados na nuvem.
    """)

    # Credenciais da API
    st.subheader("Credenciais da API")
    subdominio = st.text_input("Subdomínio API")
    usuario_api = st.text_input("Usuário API")
    senha_api = st.text_input("Senha API", type="password")

    # Credenciais do Banco de Dados
    st.subheader("Credenciais do Banco de Dados PostgreSQL")
    db_url = st.text_input("URL do Banco de Dados", type="password")

    # Parâmetros Obrigatórios
    st.subheader("Parâmetros API")
    building_id = st.text_input("Códigos das Obras (Separados por vírgula)")
    project_id = st.text_input("Códigos dos Projetos (Separados por vírgula)")
    company_id = st.text_input("Códigos das Empresas (Separados por vírgula)")

    building_id_list = [int(x)
                        for x in building_id.split(',') if x.strip().isdigit()]
    project_id_list = [int(x)
                       for x in project_id.split(',') if x.strip().isdigit()]
    company_id_list = [int(x)
                       for x in company_id.split(',') if x.strip().isdigit()]

    # Seleção dos Processos a serem executados
    st.subheader("Seleção dos Processos a serem Executados")
    processos_etl = [
        "Selecionar todos",
        "Orçamentos",
        "Insumos",
        "Pagamentos",
        "Receitas",
        "Inadimplência",
        "Movimentações Bancárias"
    ]

    # Caixa suspensa para selecionar múltiplos processos de ETL
    processos_selecionados = st.multiselect(
        "Selecione os processos que deseja executar",
        processos_etl
    )

    # Se o usuário selecionar "Selecionar todos", atribuímos todos os processos, exceto o "Selecionar todos"
    if "Selecionar todos" in processos_selecionados:
        # Seleciona todos os processos, menos o "Selecionar todos"
        processos_selecionados = processos_etl[1:]

    # Botão para iniciar os processos
    executar = st.button("Executar")
    # Botão para interromper o processo
    interromper = st.button("Interromper")

    # Inicializar a variável de interrupção no session state
    if "interromper" not in st.session_state:
        st.session_state["interromper"] = False

    # Atualizando a variável de interrupção no session state
    if interromper:
        st.session_state["interromper"] = True
        st.write("Processo interrompido.")

    # Caso o botão de "Executar" seja pressionado
    if executar:
        if not db_url:
            st.error("Por favor, insira a URL do Banco de Dados.")
            return

        # Reiniciar estado de interrupção
        st.session_state["interromper"] = False

        with st.spinner("Em andamento..."):
            try:
                if "Orçamentos" in processos_selecionados:
                    st.write("Executando ETL Orçamentos...")
                    if st.session_state["interromper"]:
                        st.warning("Processo interrompido.")
                        return
                    etl_orcamentos(
                        subdominio=subdominio,
                        usuario_api=usuario_api,
                        senha_api=senha_api,
                        building_ids=building_id_list,
                        db_url=db_url
                    )
                    st.success("ETL de Orçamentos concluído com sucesso!")

                if "Insumos" in processos_selecionados:
                    st.write("Executando ETL Insumos...")
                    if st.session_state["interromper"]:
                        st.warning("Processo interrompido.")
                        return
                    etl_insumos(
                        subdominio=subdominio,
                        usuario_api=usuario_api,
                        senha_api=senha_api,
                        building_ids=building_id_list,
                        db_url=db_url
                    )
                    st.success("ETL de Insumos concluído com sucesso!")

                if "Pagamentos" in processos_selecionados:
                    st.write("Executando ETL Pagamentos...")
                    if st.session_state["interromper"]:
                        st.warning("Processo interrompido.")
                        return
                    etl_pagamentos(
                        subdominio=subdominio,
                        usuario_api=usuario_api,
                        senha_api=senha_api,
                        company_ids=company_id_list,
                        project_ids=project_id_list,
                        db_url=db_url
                    )
                    st.success("ETL de Pagamentos concluído com sucesso!")

                if "Receitas" in processos_selecionados:
                    st.write("Executando ETL Receitas...")
                    if st.session_state["interromper"]:
                        st.warning("Processo interrompido.")
                        return
                    etl_receitas(
                        subdominio=subdominio,
                        usuario_api=usuario_api,
                        senha_api=senha_api,
                        company_ids=company_id_list,
                        db_url=db_url
                    )
                    st.success("ETL de Receitas concluído com sucesso!")

                if "Inadimplência" in processos_selecionados:
                    st.write("Executando ETL Inadimplência...")
                    if st.session_state["interromper"]:
                        st.warning("Processo interrompido.")
                        return
                    etl_inadimplencia(
                        subdominio=subdominio,
                        usuario_api=usuario_api,
                        senha_api=senha_api,
                        company_ids=company_id_list,
                        db_url=db_url
                    )
                    st.success("ETL de Inadimplência concluído com sucesso!")

                if "Movimentações Bancárias" in processos_selecionados:
                    st.write("Executando ETL Movimentações Bancárias...")
                    if st.session_state["interromper"]:
                        st.warning("Processo interrompido.")
                        return
                    etl_mov_bancarias(
                        subdominio=subdominio,
                        usuario_api=usuario_api,
                        senha_api=senha_api,
                        company_ids=company_id_list,
                        db_url=db_url
                    )
                    st.success(
                        "ETL de Movimentações Bancárias concluído com sucesso!")

            except Exception as e:
                st.error(f"Ocorreu um erro durante o processo de ETL: {e}")


if __name__ == "__main__":
    main()

# Obter o ano atual
current_year = datetime.now().year

# Adicionar rodapé com informações de direitos autorais
st.markdown(f"""
    <style>
    footer {{
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        background-color: #f1f1f1;
        text-align: center;
        padding: 10px;
        font-size: 12px;
    }}
    </style>
    <footer>
        © {current_year} - Tático Soluções. Todos os direitos reservados.
    </footer>
""", unsafe_allow_html=True)
# 🚀 Processamento de Dados

## 📌 Visão Geral
Esta aplicação desenvolvida com Streamlit executa um processo de Extração, Tratamento e Carga (ETL) dos dados extraídos de um ERP e os armazena em um Banco de Dados na nuvem. O usuário pode configurar credenciais e selecionar os processos que deseja executar.

---

## ⚙️ Funcionalidades
- Interface interativa para configuração das credenciais da API e Banco de Dados.
- Seleção de processos ETL:
  - Orçamentos
  - Insumos
  - Pagamentos
  - Receitas
  - Inadimplência
  - Movimentações Bancárias
- Opção para interromper o processo de ETL durante a execução.
- Exibição de mensagens de status para acompanhamento do processamento.

---

## 🛠 Requisitos
- Python 3.8+
- Streamlit
- Dependências do módulo `utils` (definidas no arquivo `pyproject.toml`)

---

## 📥 Instalação
1. Clone este repositório:
   ```bash
   git clone https://github.com/seu-repositorio.git
   cd seu-repositorio
   ```
2. Instale as dependências com Poetry:
   ```bash
   poetry install
   ```
3. Execute a aplicação:
   ```bash
   poetry run streamlit run app/app.py
   ```

---

## 🚀 Uso
1. Insira as credenciais da API e do Banco de Dados.
2. Informe os códigos das obras, projetos e empresas (se necessário).
3. Selecione os processos ETL desejados.
4. Clique no botão "Executar" para iniciar o processamento.
5. Para interromper o processo, clique em "Interromper".

---

## 📂 Estrutura do Projeto
```
/
├── app/                     # Módulo principal da aplicação
│   ├── __init__.py
│   ├── app.py               # Arquivo principal do Streamlit
│
├── etl/                     # Módulos de processamento ETL
│   ├── __init__.py
│   ├── etl_orcamentos.py
│   ├── etl_insumos.py
│   ├── etl_pagamentos.py
│   ├── etl_receitas.py
│   ├── etl_inadimplencia.py
│   ├── etl_mov_bancarias.py
│
├── tests/                   # Testes unitários
│   ├── __init__.py
│
├── .gitignore
├── .python-version
├── poetry.lock              # Arquivo de dependências
├── pyproject.toml           # Configuração do Poetry
├── README.md                # Documentação do projeto
```
---

## 🤝 Contribuição
Fique à vontade para abrir issues e pull requests para contribuir com melhorias no projeto!

---

## 📞 Contato
Se tiver dúvidas ou sugestões, entre em contato via:
- **Email:** seuemail@exemplo.com
- **LinkedIn:** [Seu Perfil](https://www.linkedin.com/in/c%C3%A9zarmaldini/)
- **GitHub:** [Seu GitHub](https://github.com/cezarmaldini)
# Pipeline de Dados: Integração e Modelagem Analítica de APIs

## Visão Geral do Projeto
Este projeto demonstra a construção de um pipeline de dados de ponta a ponta, com o objetivo de integrar dados de diversas APIs em uma única fonte de verdade. Ele abrange as etapas de **Extração, Carga e Transformação (ELT)**, com foco em uma arquitetura robusta e reutilizável. O pipeline utiliza dados de APIs sobre geografia e clima de países para simular um processo real de engenharia de dados.

### Arquitetura e Fluxo de Dados
O projeto segue uma arquitetura moderna de pipeline, dividida em duas camadas principais:

1.  **Camada de Ingestão (Python):** Scripts em **Python** utilizando **Jupyter Notebooks** são responsáveis por orquestrar a extração de dados de diferentes APIs,
                                  como a REST Countries, OpenWeatherMap e a do Banco Mundial. Os dados brutos são carregados para um banco de dados **PostgreSQL** hospedado na **Render**.
2.  **Camada de Transformação (dbt):** A transformação e a modelagem dos dados são realizadas com o **dbt (data build tool)**. O dbt consome os dados brutos carregados na camada anterior 
                                       e aplica transformações **SQL** para criar modelos analíticos (tabelas e visões) limpos e prontos para uso. O uso do dbt garante a documentação, a testabilidade e o versionamento das transformações.

---

## Tecnologias Utilizadas
-   **Linguagem de Programação:** Python
-   **Bibliotecas Python:** `pandas`, `requests`, `sqlalchemy`, `psycopg2`
-   **Ferramentas de Engenharia de Dados:** dbt (data build tool)
-   **Banco de Dados:** PostgreSQL (Render)
-   **Controle de Versão:** Git e GitHub
-   **APIs:** REST Countries, OpenWeatherMap e Banco Mundial

---

## Estrutura do Repositório
O projeto está organizado para refletir o fluxo do pipeline:

|-- ELT - API_PYTHON_PROJETO_PAIS/
|   |-- .ipynb                      # Notebooks com código de extração e carga.
|   |-- .csv                        # Arquivos de dados de exemplo.
|-- postgresql_dbt/                 # Projeto dbt para transformação e modelagem.
|   |-- models/                     # Modelos em SQL.
|   |-- dbt_project.yml             # Configurações do projeto.
|-- .env                            # Arquivo de variáveis de ambiente.
|-- README.md                       # Documentação do projeto.
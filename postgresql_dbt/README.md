Projeto de Análise de Dados — dbt + PostgreSQL + Render

Este projeto foi desenvolvido para praticar engenharia de dados com dbt, criando uma arquitetura de camadas (bronze, silver e gold) em um banco PostgreSQL hospedado no Render.
O objetivo é demonstrar boas práticas em modelagem de dados, versionamento com GitHub e automação de pipelines de transformação.

Objetivos do Projeto

Criar um pipeline de dados em dbt conectado a um banco PostgreSQL no Render.

Estruturar dados em camadas (bronze, silver e gold) para melhor organização e qualidade.

Consolidar tudo em tabelas de fatos e dimensões para facilitar análises.

Integrar diferentes fontes:

API REST Countries

API OpenWeatherMap

Banco Mundial (World Bank Data)

Estrutura do Projeto:

├── postgresql_dbt/           # Pasta principal do projeto dbt
│   ├── models/
│   │   ├── bronze/           # Camada de ingestão (dados brutos das APIs e tabelas)
│   │   ├── silver/           # Camada de tratamento (joins, limpeza, enriquecimento)
│   │   ├── gold/             # Camada analítica (tabelas finais para BI/insights)
│   │   └── sources.yml       # Definição das tabelas fontes
│   ├── macros/               # Funções reutilizáveis
│   ├── snapshots/            # Controle de versões de dados (se usado)
│   └── tests/                # Testes de qualidade de dados
│
├── dbt_project.yml           # Configuração principal do dbt
├── profiles.yml              # Configuração de conexão ao banco (em .dbt do usuário)
├── requirements.txt          # Dependências (dbt-core, dbt-postgres, etc.)
└── README.md                 # Este documento

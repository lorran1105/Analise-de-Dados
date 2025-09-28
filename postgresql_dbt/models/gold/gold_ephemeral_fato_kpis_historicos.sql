{{ config(
    materialized='ephemeral'
) }}

-- Este modelo efêmero prepara os dados de fatos (métricas que mudam ao longo do tempo).
-- Ele é usado para manter o código do modelo final limpo e organizado.
-- Esta abordagem é ideal para Big Data, pois evita a duplicação de dados históricos.
select
    nome_pais,
    ano,
    pib_usd,
    renda_per_capita,
    inflacao,
    crescimento_pib,
    invest_estrangeiro,
    florestas_percentual,
    energia_per_capita,
    gasto_militar_pib,
    taxa_homicidios_100mil,
    desemprego_percentual,
    expectativa_vida,
    gasto_saude_pib
from {{ ref('silver_dim_banco_mundial') }}
where nome_pais in (select distinct nome_pais from {{ ref('silver_fato_pais') }})
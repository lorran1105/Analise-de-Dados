-- Este modelo é a tabela final da camada Gold, otimizada para consumo de BI.
-- Ele combina as informações de Fato (KPIs) e Dimensão (atributos do país).
-- Usar modelos efêmeros para as fontes torna o código mais legível e eficiente,
-- já que não precisamos criar tabelas intermediárias no banco de dados.
with fatos as (
    -- Referência ao modelo efêmero que contém as métricas históricas.
    select *
    from {{ ref('gold_ephemeral_fato_kpis_historicos') }}

),

dimensoes as (
    -- Referência ao modelo efêmero que contém os atributos dos países.
    select *
    from {{ ref('gold_ephemeral_dim_paises') }}

)

select
    -- Seleciona todas as colunas da tabela de fatos
    fatos.ano,
    fatos.pib_usd,
    fatos.renda_per_capita,
    fatos.inflacao,
    fatos.crescimento_pib,
    fatos.invest_estrangeiro,
    fatos.florestas_percentual,
    fatos.energia_per_capita,
    fatos.gasto_militar_pib,
    fatos.taxa_homicidios_100mil,
    fatos.desemprego_percentual,
    fatos.expectativa_vida,
    fatos.gasto_saude_pib,

    -- Adiciona as colunas de dimensão para contextualizar
    dimensoes.nome_pais,
    dimensoes.regiao,
    dimensoes.subregiao,
    dimensoes.populacao
    
from fatos
left join dimensoes on fatos.nome_pais = dimensoes.nome_pais
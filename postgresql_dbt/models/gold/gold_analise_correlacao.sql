{{ config(
    materialized='incremental',
    unique_key=['nome_pais', 'ano_pib'] 
) }}

with fatos_numericos as (
    -- 1. Fonte de valores numéricos e YoY (para correlação)
    select * from {{ ref('gold_kpi_paises_analitico') }}
),

fatos_classificados as (
    -- 2. Fonte de classificações (as categorias)
    select * from {{ ref('gold_fatos_kpis_mais_recente') }}
),

dimensoes_pais as (
    -- 3. Fonte de atributos do país e clima
    select * from {{ ref('gold_dim_paises_analitica') }}
)

select
    -- METRICAS NUMERICAS (Alias F)
    f.ano_pib,
    f.pib_usd,
    f.pib_yoy,
    f.renda_per_capita,
    f.renda_per_capita_yoy,
    f.inflacao,
    f.inflacao_yoy,
    f.crescimento_pib,
    f.taxa_homicidios_100mil,
    f.florestas_percentual,
    f.expectativa_vida,

    -- CLASSIFICAÇÕES (Alias C) - Agora com a fonte correta
    c.categoria_renda,
    c.nivel_inflacao,
    c.crescimento_pib_categoria,
    c.nivel_violencia,
    c.cobertura_florestal,
    c.categoria_expectativa_vida,

    -- DIMENSÕES (Alias D)
    d.nome_pais,
    d.regiao,
    d.subregiao,
    d.densidade_populacional_categoria,
    d.tamanho_pais_categoria,
    d.temperatura,
    d.umidade

-- 1. Junta Fatos Numéricos (F) com Dimensões (D)
from fatos_numericos f
left join dimensoes_pais d
    on f.nome_pais = d.nome_pais

-- 2. Junta Fatos Numéricos (F) com as Classificações (C)
left join fatos_classificados c
    on f.nome_pais = c.nome_pais
    -- O 'ano' da tabela de classificação (C) deve ser igual a um dos anos da tabela numérica (F)
    and c.ano = f.ano_pib 

{% if is_incremental() %}
    -- Filtro incremental usando a nova chave: 'ano_pib'
    where f.ano_pib > (select max(ano_pib) from {{ this }})
{% endif %}
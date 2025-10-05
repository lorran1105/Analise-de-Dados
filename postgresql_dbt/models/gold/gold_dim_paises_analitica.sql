{{ config(
    materialized='table'
) }}

-- ============================================================
-- OBJETIVO:
-- Criar uma tabela GOLD que consolida as principais informações
-- históricas de cada país, unindo dados demográficos, econômicos
-- e climáticos, com cálculo de variações ano a ano (YoY).
--
-- Este modelo fornece a base para análises temporais e correlações
-- entre fatores econômicos, sociais e ambientais.
-- ============================================================

with base as (
    -- Fonte primária: Banco Mundial (indicadores históricos)
    select *
    from {{ ref('silver_dim_banco_mundial') }}
),

-- ============================================================
-- 1. Cálculo das variações YoY (usando a base histórica completa)
-- ============================================================
bm_yoy as (
    select
        nome_pais,
        ano,
        pib_usd,
        round(((pib_usd - lag(pib_usd) over(partition by nome_pais order by ano))
              / nullif(lag(pib_usd) over(partition by nome_pais order by ano), 0)) * 100, 2) as pib_yoy,

        renda_per_capita,
        round(((renda_per_capita - lag(renda_per_capita) over(partition by nome_pais order by ano))
              / nullif(lag(renda_per_capita) over(partition by nome_pais order by ano), 0)) * 100, 2) as renda_per_capita_yoy,

        inflacao,
        round(((inflacao - lag(inflacao) over(partition by nome_pais order by ano))
              / nullif(lag(inflacao) over(partition by nome_pais order by ano), 0)) * 100, 2) as inflacao_yoy,

        crescimento_pib,
        round(((crescimento_pib - lag(crescimento_pib) over(partition by nome_pais order by ano))
              / nullif(lag(crescimento_pib) over(partition by nome_pais order by ano), 0)) * 100, 2) as crescimento_pib_yoy,

        taxa_homicidios_100mil,
        round(((taxa_homicidios_100mil - lag(taxa_homicidios_100mil) over(partition by nome_pais order by ano))
              / nullif(lag(taxa_homicidios_100mil) over(partition by nome_pais order by ano), 0)) * 100, 2) as taxa_homicidios_100mil_yoy,

        florestas_percentual,
        round(((florestas_percentual - lag(florestas_percentual) over(partition by nome_pais order by ano))
              / nullif(lag(florestas_percentual) over(partition by nome_pais order by ano), 0)) * 100, 2) as florestas_percentual_yoy,

        expectativa_vida,
        round(((expectativa_vida - lag(expectativa_vida) over(partition by nome_pais order by ano))
              / nullif(lag(expectativa_vida) over(partition by nome_pais order by ano), 0)) * 100, 2) as expectativa_vida_yoy
    from base
),

-- ============================================================
-- 2. Enriquecimento com dimensões auxiliares
-- ============================================================
bm_enriquecido as (
    select distinct
        -- ==========================================
        -- INFORMAÇÕES DEMOGRÁFICAS E GEOGRÁFICAS
        -- ==========================================
        f.nome_pais,
        f.capital,
        f.regiao,
        f.subregiao,
        f.populacao,
        f.area_km2,

        -- Classificações derivadas via macros
        {{ classificar_densidade_populacional('f.populacao', 'f.area_km2') }} as densidade_populacional_categoria,
        {{ classificar_tamanho_pais('f.area_km2') }} as tamanho_pais_categoria,

        -- ==========================================
        -- IDENTIFICAÇÃO E METADADOS
        -- ==========================================
        f.idiomas,
        f.moedas,
        f.bandeira_url,
        p.codigo_alpha2,
        p.codigo_alpha3,

        -- ==========================================
        -- INDICADORES ECONÔMICOS E SOCIAIS
        -- ==========================================
        y.ano as ano_pib,
        y.pib_usd,
        y.pib_yoy,
        y.renda_per_capita,
        y.renda_per_capita_yoy,
        y.inflacao,
        y.inflacao_yoy,
        y.crescimento_pib,
        y.crescimento_pib_yoy,
        y.taxa_homicidios_100mil,
        y.taxa_homicidios_100mil_yoy,
        y.florestas_percentual,
        y.florestas_percentual_yoy,
        y.expectativa_vida,
        y.expectativa_vida_yoy,

        -- ==========================================
        -- METADADOS DE PROCESSAMENTO
        -- ==========================================
        current_timestamp as data_processamento

    from {{ ref('silver_fato_pais') }} f
    left join {{ ref('silver_dim_pais') }} p
        on f.nome_oficial = p.nome_oficial
    left join bm_yoy y
        on f.nome_pais = y.nome_pais
)

select * from bm_enriquecido

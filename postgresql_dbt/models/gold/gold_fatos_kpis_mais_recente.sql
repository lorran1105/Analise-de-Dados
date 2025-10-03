{{ config(
    materialized='table'
) }}

-- CTE para coletar a informação mais recente de cada KPI por país.
-- ROW_NUMBER é muito mais eficiente do que múltiplos JOINs para esta tarefa.
with bm_ranked as (
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
        gasto_saude_pib,
        
        -- A função de janela ROW_NUMBER atribui um rank (1, 2, 3...)
        -- para cada registro de um país, ordenando do ano mais recente para o mais antigo.
        row_number() over (partition by nome_pais order by ano desc) as rn
        
    from {{ ref('silver_dim_banco_mundial') }}
),

-- Aplica as classificações e remove as colunas numéricas brutas.
kpis_classificados as (
    select
        nome_pais,
        ano,
        
        -- Macros para classificar os KPIs
        {{ classificar_renda('renda_per_capita') }} as categoria_renda,
        {{ classificar_inflacao('inflacao') }} as nivel_inflacao,
        {{ classificar_crescimento_pib('crescimento_pib') }} as crescimento_pib_categoria,
        {{ classificar_nivel_violencia('taxa_homicidios_100mil') }} as nivel_violencia,
        {{ classificar_cobertura_florestal('florestas_percentual') }} as cobertura_florestal,
        {{ classificar_expectativa_vida('expectativa_vida') }} as categoria_expectativa_vida
        
    from bm_ranked
    where rn = 1
)

select * from kpis_classificados
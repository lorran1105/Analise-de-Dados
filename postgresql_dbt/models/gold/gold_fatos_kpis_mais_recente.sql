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
        
        -- Classificação de Renda (baseada no Banco Mundial)
        case
            when renda_per_capita >= 13846 then 'Renda Alta'
            when renda_per_capita >= 4466 then 'Renda Media-Alta'
            when renda_per_capita >= 1136 then 'Renda Media-Baixa'
            else 'Renda Baixa'
        end as categoria_renda,

        -- Classificação de Inflação
        case
            when inflacao >= 20 then 'Hiperinflacao'
            when inflacao >= 5 then 'Alta Inflacao'
            when inflacao >= 0 then 'Inflacao Moderada'
            else 'Deflacao/Estagnacao'
        end as nivel_inflacao,
        
        -- Classificação de Crescimento do PIB
        case
            when crescimento_pib < 0 then 'Recessao'
            when crescimento_pib >= 5 then 'Forte Crescimento'
            when crescimento_pib >= 2 then 'Crescimento Moderado'
            else 'Estagnacao Economica'
        end as crescimento_pib_categoria,
        
        -- Classificação de Violência
        case
            when taxa_homicidios_100mil >= 20 then 'Muito Violento'
            when taxa_homicidios_100mil >= 10 then 'Violento'
            when taxa_homicidios_100mil >= 5 then 'Moderado'
            when taxa_homicidios_100mil > 0 then 'Pouco Violento'
            else 'Sem Violencia Registrada'
        end as nivel_violencia,
        
        -- Classificação de Cobertura Florestal
        case
            when florestas_percentual >= 50 then 'Muita Floresta'
            when florestas_percentual >= 30 then 'Floresta Consideravel'
            when florestas_percentual >= 10 then 'Pouca Floresta'
            else 'Muito Pouca Floresta'
        end as cobertura_florestal,

        -- Classificação de Expectativa de Vida
        case
            when expectativa_vida >= 80 then 'Alta Expectativa de Vida'
            when expectativa_vida >= 70 then 'Media Expectativa de Vida'
            else 'Baixa Expectativa de Vida'
        end as categoria_expectativa_vida
        
    from bm_ranked
    where rn = 1
)

select *
from kpis_classificados
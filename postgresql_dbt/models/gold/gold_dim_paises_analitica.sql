{{ config(
    materialized='table'
) }}

select
    f.nome_pais,
    f.capital,
    f.regiao,
    f.subregiao,
    f.populacao,
    f.area_km2,
    
    -- Classificação de densidade populacional
    case
        when (cast(f.populacao as numeric) / f.area_km2) > 500 then 'Populacao Densa'
        when (cast(f.populacao as numeric) / f.area_km2) > 100 then 'Populacao Moderada'
        when (cast(f.populacao as numeric) / f.area_km2) > 10 then 'Populacao Baixa'
        else 'Populacao Muito Baixa'
    end as densidade_populacional_categoria,

    -- Classificação de tamanho do país
    case
        when f.area_km2 > 2000000 then 'Muito Grande'
        when f.area_km2 > 200000 then 'Grande'
        when f.area_km2 > 20000 then 'Medio'
        else 'Pequeno'
    end as tamanho_pais_categoria,

    c.temperatura,
    c.sensacao_termica,
    c.umidade,
    c.descricao_clima
    
from {{ ref('silver_fato_pais') }} f
left join {{ ref('silver_dim_clima_capital') }} c
    on f.nome_resumido = c.codigo_pais
where f.populacao is not null
  and f.area_km2 is not null
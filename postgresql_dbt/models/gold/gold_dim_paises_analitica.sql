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
    
    -- usando a macro para classificar a densidade populacional
    {{ classificar_densidade_populacional('f.populacao', 'f.area_km2') }} as densidade_populacional_categoria,

    -- usando a macro para classificar o tamanho do país
    {{ classificar_tamanho_pais('f.area_km2') }} as tamanho_pais_categoria,

    c.temperatura,
    c.sensacao_termica,
    c.umidade,
    c.descricao_clima
    
from {{ ref('silver_fato_pais') }} f
left join {{ ref('silver_dim_clima_capital') }} c
    -- CORREÇÃO: Junta os códigos Alpha-2 do país
    on f.nome_resumido = c.codigo_pais 
where c.rn = 1
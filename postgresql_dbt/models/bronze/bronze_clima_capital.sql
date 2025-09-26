


with source as (

SELECT * FROM  {{ source('projeto_paises', 'tbl_clima_capital_pais') }}
),

renamed as (

select
    cidade,
    codigo_pais,
    temperatura,
    sensacao_termica,
    temperatura_min,
    temperatura_max,
    umidade,
    descricao_clima,
    horario_medicao
FROM source


)

select * from renamed



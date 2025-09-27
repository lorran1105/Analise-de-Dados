with bronze as (

    select * 
    from {{ ref('bronze_clima_capital') }}

),

tratado as (

   SELECT
    upper(trim(cidade)) AS cidade,
    upper(trim(codigo_pais)) AS codigo_pais,
    cast(temperatura AS numeric) AS temperatura,
    cast(sensacao_termica AS numeric) AS sensacao_termica,
    cast(temperatura_min AS numeric) AS temperatura_min,
    cast(temperatura_max AS numeric) AS temperatura_max,
    cast(umidade AS int) AS umidade,
    initcap(trim(descricao_clima)) AS descricao_clima,
    TO_CHAR(TO_TIMESTAMP(horario_medicao, 'DD/MM/YYYY HH24:MI:SS'), 'DD/MM/YYYY HH24:MI:SS') AS horario_medicao,
    row_number() OVER(PARTITION BY codigo_pais ORDER BY TO_TIMESTAMP(horario_medicao, 'DD/MM/YYYY HH24:MI:SS') DESC) AS rn
    from bronze

)

select *
from tratado
where rn = 1

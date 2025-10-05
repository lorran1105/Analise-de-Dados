{{ config(
    materialized='table'
) }}


select
    count(*) as total_registros,
    count(nome_pais) as paises_com_nome,
    count(*) - count(nome_pais) as paises_sem_nome
FROM {{ ref('gold_analise_correlacao') }} 
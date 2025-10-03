{{ config(
    materialized='table'
) }}

SELECT
    f.nome_pais
FROM {{ ref('silver_fato_pais') }} f
LEFT JOIN {{ ref('silver_dim_banco_mundial') }} bm
    ON f.nome_pais = bm.nome_pais
WHERE bm.nome_pais IS NULL
{{ config(
    materialized='ephemeral'
) }}

-- Este modelo efêmero atua como um CTE global.
-- Ele não cria uma tabela no banco de dados, o que otimiza o pipeline.
-- O dbt irá injetar este código SQL diretamente no modelo que o referencia.
select
    nome_pais,
    nome_oficial,
    capital,
    regiao,
    subregiao,
    populacao,
    area_km2,
    idiomas,
    moedas,
    bandeira_url
from {{ ref('silver_fato_pais') }}
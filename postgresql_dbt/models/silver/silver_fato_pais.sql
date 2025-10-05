{{ config( materialized='view') }}

with bronze as (
    select * from {{ ref('bronze_fato_pais') }}
),

tratado as (
    select
        upper(trim(nome_resumido)) as nome_resumido,
        
        -- CORREÇÃO: COALESCE com 3 níveis
        -- 1. Tenta a coluna 'nome'.
        -- 2. Se for nulo/vazio, tenta 'nome_oficial'.
        -- 3. Se for nulo/vazio, usa o 'nome_resumido' (código 2 letras) como garantia.
        upper(trim(
            coalesce(
                nullif(trim(nome), ''),      
                nullif(trim(nome_oficial), ''),
                nome_resumido 
            )
        )) as nome_pais,
        
        upper(trim(nome_oficial)) as nome_oficial,
        upper(trim(capital)) as capital,
        upper(trim(regiao)) as regiao,
        upper(trim(subregiao)) as subregiao,
        cast(populacao as bigint) as populacao,
        cast(area_km2 as numeric) as area_km2,
        coalesce(nullif(trim(idiomas), ''), 'DESCONHECIDO') as idiomas,
        coalesce(nullif(trim(moedas), ''), 'DESCONHECIDO') as moedas,
        trim(bandeira_url) as bandeira_url,
        cast(data_carga as timestamp) as data_carga
    from bronze
)

select * from tratado
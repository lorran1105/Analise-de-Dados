{{ config(
    materialized='ephemeral'
) }}

with bronze as (
    select * from {{ ref('bronze_dim_pais') }}
),

tratado as (
    select
        upper(trim(nome_pais)) as nome_pais,
        upper(trim(official_name)) as nome_oficial,
        upper(trim(codigo_alpha2)) as codigo_alpha2,
        upper(trim(codigo_alpha3)) as codigo_alpha3,
        cast(nullif(codigo_numerico, '') as int) as codigo_numerico,
        upper(trim(codigo_olimpico)) as codigo_olimpico,
        cast(data_carga as timestamp) as data_carga,
        row_number() over(partition by codigo_alpha3 order by data_carga desc) as rn
    from bronze
)

select * from tratado where rn = 1
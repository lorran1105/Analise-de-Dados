with bronze as (

    select * 
    from {{ ref('bronze_dim_pais') }}

),

tratado as (

    select
    upper(trim(nome_pais)) AS nome_pais,
    upper(trim(official_name)) AS nome_oficial,
    upper(trim(codigo_alpha2)) AS codigo_alpha2,
    upper(trim(codigo_alpha3)) AS codigo_alpha3,
    cast(NULLIF(codigo_numerico, '') AS int) AS codigo_numerico,
    upper(trim(codigo_olimpico)) AS codigo_olimpico,
    cast(data_carga AS timestamp) AS data_carga,
    row_number() OVER(PARTITION BY codigo_alpha3 ORDER BY data_carga DESC) AS rn
    from bronze

)

select *
from tratado
where rn = 1

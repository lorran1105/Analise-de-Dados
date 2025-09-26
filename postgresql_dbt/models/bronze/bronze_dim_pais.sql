
with source as (

SELECT * from {{ source('projeto_paises', 'tbl_dim_pais') }}
),

renamed as (

    select
    nome as nome_pais,
    codigo_alpha2,
    codigo_alpha3,
    codigo_numerico,
    codigo_olimpico,
    official_name,
    data_carga
from source

)

select * from renamed






with source as (

select * from {{ source('projeto_paises', 'fato_pais') }}
),

renamed as (


select *   from source

)

select * from renamed




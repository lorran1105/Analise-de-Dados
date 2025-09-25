

with source as (

SELECT nome_resumido, nome, nome_oficial, capital, regiao, subregiao, populacao, area_km2, idiomas, moedas, bandeira_url, data_carga
FROM {{source('projeto_paises','fato_pais')}}
),

renamed as (
select count (*) as qtd from source

)

select * from renamed



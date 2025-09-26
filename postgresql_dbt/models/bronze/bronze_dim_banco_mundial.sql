with source as (

SELECT * FROM {{ source('projeto_paises', 'tbl_dados_banco_mundial') }}
),

renamed as (

   SELECT 
   country country_name ,
    date,
    "PIB_USD",
    "Renda_per_capita",
    "Inflacao",
    "Crescimento_PIB",
    "Invest_Estrangeiro",
    "Florestas_percentual",
    "Energia_per_capita",
    "Gasto_Militar_PIB",
    "Taxa_Homicidios_100mil",
    "Desemprego_percentual",
    "Expectativa_vida",
    "Gasto_saude_PIB"
    FROM source


)

select * from renamed





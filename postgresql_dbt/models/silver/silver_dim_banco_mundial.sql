with bronze as (

    select * 
    from {{ ref('bronze_dim_banco_mundial') }}

),

tratado as (

    select
        upper(trim(country_name)) as nome_pais,
        extract(year from date) as ano,
		cast("PIB_USD" as numeric) as pib_usd,
        cast("Renda_per_capita" as numeric) as renda_per_capita,
        cast("Inflacao" as numeric) as inflacao,
        cast("Crescimento_PIB" as numeric) as crescimento_pib,
        cast("Invest_Estrangeiro" as numeric) as invest_estrangeiro,
        cast("Florestas_percentual" as numeric) as florestas_percentual,
        cast("Energia_per_capita" as numeric) as energia_per_capita,
        cast("Gasto_Militar_PIB" as numeric) as gasto_militar_pib,
        cast("Taxa_Homicidios_100mil" as numeric) as taxa_homicidios_100mil,
        cast("Desemprego_percentual" as numeric) as desemprego_percentual,
        cast("Expectativa_vida" as numeric) as expectativa_vida,
        cast("Gasto_saude_PIB" as numeric) as gasto_saude_pib,
        TO_CHAR(data_extracao, 'DD/MM/YYYY HH24:MI:SS') AS data_extracao
    from bronze

)

select * from tratado

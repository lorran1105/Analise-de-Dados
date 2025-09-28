with bronze as (

    select * 
    from {{ ref('bronze_dim_banco_mundial') }}

),

tratado as (

    select
        -- Nome original vindo da API (mantemos para auditoria)
        upper(trim(country_name)) as nome_original,

        -- Nome tratado / normalizado
        case 
            when upper(trim(country_name)) = 'VENEZUELA, RB' then 'VENEZUELA'
            when upper(trim(country_name)) = 'BAHAMAS, THE' then 'BAHAMAS'
            when upper(trim(country_name)) = 'EGYPT, ARAB REP.' then 'EGYPT'
            when upper(trim(country_name)) = 'IRAN, ISLAMIC REP.' then 'IRAN'
            when upper(trim(country_name)) = 'HONG KONG SAR, CHINA' then 'HONG KONG'
            when upper(trim(country_name)) = 'KOREA, REP.' then 'SOUTH KOREA'
            when upper(trim(country_name)) like '%KOREA, DEM. PEOPLE%' then 'NORTH KOREA'
            when upper(trim(country_name)) = 'SYRIAN ARAB REPUBLIC' then 'SYRIA'
            when upper(trim(country_name)) = 'Viet Nam' then 'VIETNAM'
            else upper(trim(country_name))
        end as nome_pais,
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
        cast(data_extracao as timestamp) as data_extracao
    from bronze

)

select * from tratado

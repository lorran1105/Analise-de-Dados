{{ config(materialized='view') }}

with bronze as (

    select * from {{ ref('bronze_dim_banco_mundial') }}

),

tratado as (

    select
        -- Nome original vindo da API (mantemos para auditoria)
        upper(trim(country_name)) as nome_original,

        -- Nome tratado / normalizado: Mapeamento de nomes para padronização
        case 
            -- CORREÇÕES BASE (Inclusão de TURKIYE, RUSSIAN FEDERATION, etc.)
            when upper(trim(country_name)) = 'VENEZUELA, RB' then 'VENEZUELA'
            when upper(trim(country_name)) = 'BAHAMAS, THE' then 'BAHAMAS'
            when upper(trim(country_name)) = 'EGYPT, ARAB REP.' then 'EGYPT'
            when upper(trim(country_name)) = 'IRAN, ISLAMIC REP.' then 'IRAN'
            when upper(trim(country_name)) = 'HONG KONG SAR, CHINA' then 'HONG KONG'
            when upper(trim(country_name)) = 'KOREA, REP.' then 'SOUTH KOREA'
            when upper(trim(country_name)) like '%KOREA, DEM. PEOPLE%' then 'NORTH KOREA'
            when upper(trim(country_name)) = 'SYRIAN ARAB REPUBLIC' then 'SYRIA'
            when upper(trim(country_name)) = 'VIET NAM' then 'VIETNAM'
            when upper(trim(country_name)) = 'TURKIYE' then 'TURKEY'
            when upper(trim(country_name)) = 'RUSSIAN FEDERATION' then 'RUSSIA'
            when upper(trim(country_name)) = 'YEMEN, REP.' then 'YEMEN'
            when upper(trim(country_name)) = 'BRUNEI DARUSSALAM' then 'BRUNEI'
            when upper(trim(country_name)) = 'LAO PDR' then 'LAOS'
            when upper(trim(country_name)) = 'SLOVAK REPUBLIC' then 'SLOVAKIA'
            when upper(trim(country_name)) = 'CONGO, DEM. REP.' then 'DR CONGO'
            when upper(trim(country_name)) = 'CONGO, REP.' then 'REPUBLIC OF THE CONGO'
            when upper(trim(country_name)) = 'KYRGYZ REPUBLIC' then 'KYRGYZSTAN'
            when upper(trim(country_name)) = 'MICRONESIA, FED. STS.' then 'MICRONESIA'
            when upper(trim(country_name)) = 'GAMBIA, THE' then 'GAMBIA'
            
            -- NOVAS CORREÇÕES (Para resolver os desvios restantes)
            when upper(trim(country_name)) = 'CÔTE D''IVOIRE' then 'IVORY COAST'  -- Mapeia Costa do Marfim
            when upper(trim(country_name)) = 'CABO VERDE' then 'CAPE VERDE'        -- Mapeia Cabo Verde (em português no BM)
            when upper(trim(country_name)) = 'WEST BANK AND GAZA' then 'PALESTINE' -- Mapeia Palestina (nome do BM)
            when upper(trim(country_name)) = 'SÃO TOMÉ AND PRINCIPE' then 'SÃO TOMÉ AND PRÍNCIPE' -- Mapeia São Tomé
            when upper(trim(country_name)) = 'TAIWAN, CHINA' then 'TAIWAN'        -- Mapeia Taiwan
            when upper(trim(country_name)) = 'ST. LUCIA' then 'SAINT LUCIA'
            when upper(trim(country_name)) = 'ST. VINCENT AND THE GRENADINES' then 'SAINT VINCENT AND THE GRENADINES'
            when upper(trim(country_name)) = 'ST. KITTS AND NEVIS' then 'SAINT KITTS AND NEVIS'
            
            -- Se não for nenhum dos casos acima, o nome é mantido como está
            else upper(trim(country_name))
        end as nome_pais,
        
        -- Colunas de dados e tipagem
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
        cast("Gasto_saude_PIB" as numeric) as gasto_saude_pib
        
    from bronze
)

select * from tratado
-- FILTRO IMPORTANTE: Remove grupos regionais e agregações do Banco Mundial (que não são países individuais)
where nome_original not in (
    -- Agregações Regionais e Econômicas do Banco Mundial
    'AFRICA EASTERN AND SOUTHERN', 'AFRICA WESTERN AND CENTRAL', 'ARAB WORLD', 
    'CARIBBEAN SMALL STATES', 'CENTRAL ASIA', 'EAST ASIA & PACIFIC', 'EAST ASIA & PACIFIC (EXCLUDING HIGH INCOME)',
    'EURO AREA', 'EUROPE & CENTRAL ASIA', 'EUROPE & CENTRAL ASIA (EXCLUDING HIGH INCOME)', 
    'EUROPEAN UNION', 'HIGH INCOME', 'IDA COUNTRIES', 'IDA TOTAL', 'IDA BLEND', 'IDA ONLY', 
    'LATIN AMERICA & CARIBBEAN', 'LATIN AMERICA & CARIBBEAN (EXCLUDING HIGH INCOME)', 
    'LEAST DEVELOPED COUNTRIES', 'LOW & MIDDLE INCOME', 'LOW INCOME', 'LOWER MIDDLE INCOME',
    'MIDDLE EAST & NORTH AFRICA', 'MIDDLE EAST & NORTH AFRICA (EXCLUDING HIGH INCOME)', 
    'MIDDLE INCOME', 'NORTH AMERICA', 'OECD MEMBERS', 'OTHER SMALL STATES', 'PACIFIC ISLAND SMALL STATES',
    'SMALL STATES', 'SOUTH ASIA', 'SUB-SAHARAN AFRICA', 'SUB-SAHARAN AFRICA (EXCLUDING HIGH INCOME)', 
    'UPPER MIDDLE INCOME', 'WORLD'
)
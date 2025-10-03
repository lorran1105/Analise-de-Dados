{{ config(
    materialized='ephemeral'
) }}

with bronze as (

    select * from {{ ref('bronze_dim_banco_mundial') }}

),

tratado as (

    select
        -- Nome original vindo da API 
        upper(trim(country_name)) as nome_original,

        -- Nome tratado / normalizado: Mapeamento de nomes para padronização
        case 
            -- CORREÇÕES 1
            when upper(trim(country_name)) = 'VENEZUELA, RB' then 'VENEZUELA'
            when upper(trim(country_name)) = 'BAHAMAS, THE' then 'BAHAMAS'
            when upper(trim(country_name)) = 'EGYPT, ARAB REP.' then 'EGYPT'
            when upper(trim(country_name)) = 'IRAN, ISLAMIC REP.' then 'IRAN'
            when upper(trim(country_name)) = 'HONG KONG SAR, CHINA' then 'HONG KONG'
            when upper(trim(country_name)) = 'KOREA, REP.' then 'SOUTH KOREA'
            when upper(trim(country_name)) like '%KOREA, DEM. PEOPLE%' then 'NORTH KOREA'
            when upper(trim(country_name)) = 'SYRIAN ARAB REPUBLIC' then 'SYRIA'
            when upper(trim(country_name)) = 'VIET NAM' then 'VIETNAM'
            
            --  CORREÇÕES 2 (Baseadas na lista de desvios)
            when upper(trim(country_name)) = 'YEMEN, REP.' then 'YEMEN'
            when upper(trim(country_name)) = 'TURKIYE' then 'TURKEY'
            when upper(trim(country_name)) = 'BRUNEI DARUSSALAM' then 'BRUNEI'
            when upper(trim(country_name)) = 'LAO PDR' then 'LAOS'
            when upper(trim(country_name)) = 'SLOVAK REPUBLIC' then 'SLOVAKIA'
            when upper(trim(country_name)) = 'KYRGYZ REPUBLIC' then 'KYRGYZSTAN'
            when upper(trim(country_name)) = 'RUSSIAN FEDERATION' then 'RUSSIA'
            when upper(trim(country_name)) = 'CURACAO' then 'CURAÇAO'
            when upper(trim(country_name)) = 'MICRONESIA, FED. STS.' then 'MICRONESIA'
            when upper(trim(country_name)) = 'GAMBIA, THE' then 'GAMBIA'
            
            -- Correções para os Congo e Costa do Marfim
            when upper(trim(country_name)) = 'CONGO, DEM. REP.' then 'DR CONGO'
            when upper(trim(country_name)) = 'CONGO, REP.' then 'REPUBLIC OF THE CONGO'
            when upper(trim(country_name)) = 'CÔTE D''IVOIRE' then 'IVORY COAST'
            
            -- Taiwan e outros que podem ser mapeados, mesmo que os dados sejam escassos
            when upper(trim(country_name)) = 'TAIWAN, CHINA' then 'TAIWAN'
            
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
-- NOVO FILTRO: Remove grupos regionais e agregações do Banco Mundial
where nome_original not in (
    'AFRICA EASTERN AND SOUTHERN',
    'AFRICA WESTERN AND CENTRAL',
    'EAST ASIA & PACIFIC',
    'EUROPE & CENTRAL ASIA',
    'HIGH INCOME',
    'LOWER MIDDLE INCOME',
    'MIDDLE EAST & NORTH AFRICA',
    'MIDDLE INCOME',
    'NORTH AMERICA',
    'SOUTH ASIA',
    'UPPER MIDDLE INCOME',
    -- Adicione quaisquer outros grupos não-países que aparecerem nos seus dados
    'WORLD',
    'EUROPEAN UNION',
    'SUB-SAHARAN AFRICA'
)
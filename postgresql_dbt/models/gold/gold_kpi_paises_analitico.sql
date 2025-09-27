with fato as (

    select *
    from {{ ref('silver_fato_pais') }}

),

dim_pais as (

    select *
    from {{ ref('silver_dim_pais') }}

),

dim_clima as (

    select *
    from {{ ref('silver_dim_clima_capital') }}

),

bm as (

    select
        nome_pais,
        ano,
        pib_usd,
        renda_per_capita,
        inflacao,
        crescimento_pib,
        invest_estrangeiro,
        florestas_percentual,
        energia_per_capita,
        gasto_militar_pib,
        taxa_homicidios_100mil,
        desemprego_percentual,
        expectativa_vida,
        gasto_saude_pib
    from {{ ref('silver_dim_banco_mundial') }}
),

-- Identifica o último ano de cada país
ultimo_ano as (
    select 
        nome_pais,
        max(ano) as ano_max
    from bm
    group by nome_pais
),

-- Traz último ano e o anterior para cálculo do YoY
bm_comparativo as (
    select
        b.*,
        lag(b.pib_usd) over(partition by b.nome_pais order by b.ano) as pib_usd_prev,
        lag(b.renda_per_capita) over(partition by b.nome_pais order by b.ano) as renda_prev,
        lag(b.inflacao) over(partition by b.nome_pais order by b.ano) as inflacao_prev,
        lag(b.crescimento_pib) over(partition by b.nome_pais order by b.ano) as crescimento_prev,
        lag(b.invest_estrangeiro) over(partition by b.nome_pais order by b.ano) as invest_prev,
        lag(b.florestas_percentual) over(partition by b.nome_pais order by b.ano) as florestas_prev,
        lag(b.energia_per_capita) over(partition by b.nome_pais order by b.ano) as energia_prev,
        lag(b.gasto_militar_pib) over(partition by b.nome_pais order by b.ano) as militar_prev,
        lag(b.taxa_homicidios_100mil) over(partition by b.nome_pais order by b.ano) as homicidios_prev,
        lag(b.desemprego_percentual) over(partition by b.nome_pais order by b.ano) as desemprego_prev,
        lag(b.expectativa_vida) over(partition by b.nome_pais order by b.ano) as expectativa_prev,
        lag(b.gasto_saude_pib) over(partition by b.nome_pais order by b.ano) as saude_prev
    from bm b
),

-- Mantém só o último ano e calcula YoY
bm_yoy as (
    select
        b.nome_pais,
        b.ano,

        -- KPIs valor do último ano
        b.pib_usd,
        b.renda_per_capita,
        b.inflacao,
        b.crescimento_pib,
        b.invest_estrangeiro,
        b.florestas_percentual,
        b.energia_per_capita,
        b.gasto_militar_pib,
        b.taxa_homicidios_100mil,
        b.desemprego_percentual,
        b.expectativa_vida,
        b.gasto_saude_pib,

        -- KPIs YoY
        round(((b.pib_usd - b.pib_usd_prev) / nullif(b.pib_usd_prev,0)) * 100, 2) as pib_yoy,
        round(((b.renda_per_capita - b.renda_prev) / nullif(b.renda_prev,0)) * 100, 2) as renda_yoy,
        round(((b.inflacao - b.inflacao_prev) / nullif(b.inflacao_prev,0)) * 100, 2) as inflacao_yoy,
        round(((b.crescimento_pib - b.crescimento_prev) / nullif(b.crescimento_prev,0)) * 100, 2) as crescimento_pib_yoy,
        round(((b.invest_estrangeiro - b.invest_prev) / nullif(b.invest_prev,0)) * 100, 2) as invest_estrangeiro_yoy,
        round(((b.florestas_percentual - b.florestas_prev) / nullif(b.florestas_prev,0)) * 100, 2) as florestas_percentual_yoy,
        round(((b.energia_per_capita - b.energia_prev) / nullif(b.energia_prev,0)) * 100, 2) as energia_per_capita_yoy,
        round(((b.gasto_militar_pib - b.militar_prev) / nullif(b.militar_prev,0)) * 100, 2) as gasto_militar_pib_yoy,
        round(((b.taxa_homicidios_100mil - b.homicidios_prev) / nullif(b.homicidios_prev,0)) * 100, 2) as taxa_homicidios_yoy,
        round(((b.desemprego_percentual - b.desemprego_prev) / nullif(b.desemprego_prev,0)) * 100, 2) as desemprego_yoy,
        round(((b.expectativa_vida - b.expectativa_prev) / nullif(b.expectativa_prev,0)) * 100, 2) as expectativa_vida_yoy,
        round(((b.gasto_saude_pib - b.saude_prev) / nullif(b.saude_prev,0)) * 100, 2) as gasto_saude_yoy
    from bm_comparativo b
    join ultimo_ano u
      on b.nome_pais = u.nome_pais
     and b.ano = u.ano_max
)

-- União final
select
    f.nome_pais,
    f.capital,
    f.regiao,
    f.subregiao,
    f.populacao,
    f.area_km2,
    f.idiomas,
    f.moedas,
    f.bandeira_url,

    p.codigo_alpha2,
    p.codigo_alpha3,
    p.codigo_numerico,
    p.codigo_olimpico,

    c.temperatura,
    c.sensacao_termica,
    c.umidade,
    c.descricao_clima,

    y.ano as ano_atual,

    -- KPIs + YoY
    y.pib_usd,              y.pib_yoy,
    y.renda_per_capita,     y.renda_yoy,
    y.inflacao,             y.inflacao_yoy,
    y.crescimento_pib,      y.crescimento_pib_yoy,
    y.invest_estrangeiro,   y.invest_estrangeiro_yoy,
    y.florestas_percentual, y.florestas_percentual_yoy,
    y.energia_per_capita,   y.energia_per_capita_yoy,
    y.gasto_militar_pib,    y.gasto_militar_pib_yoy,
    y.taxa_homicidios_100mil, y.taxa_homicidios_yoy,
    y.desemprego_percentual, y.desemprego_yoy,
    y.expectativa_vida,     y.expectativa_vida_yoy,
    y.gasto_saude_pib,      y.gasto_saude_yoy

from fato f
left join dim_pais p
    on f.nome_oficial = p.nome_oficial
left join dim_clima c
    on f.nome_resumido = c.codigo_pais
left join bm_yoy y
    on f.nome_pais = y.nome_pais
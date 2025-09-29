--
-- Este modelo cria uma tabela de fatos unificada, pronta para análise.
-- Ele consolida dados demográficos, climáticos e KPIs econômicos
-- mais recentes de cada país em uma única linha, facilitando o consumo
-- em ferramentas de Business Intelligence.
--

-- CTEs (Common Table Expressions) para referenciar as tabelas Silver de origem
with fato as (
    -- Referencia a tabela fato principal com dados como população, área e região.
    select *
    from {{ ref('silver_fato_pais') }}
),

dim_pais as (
    -- Referencia a dimensão de países com códigos padronizados (Alpha2, Alpha3, etc.).
    select *
    from {{ ref('silver_dim_pais') }}
),

dim_clima as (
    -- Referencia a dimensão de clima com os dados mais recentes das capitais.
    select *
    from {{ ref('silver_dim_clima_capital') }}
),

bm as (
    -- Referencia a dimensão do Banco Mundial, que contém o histórico completo de KPIs.
    select *
    from {{ ref('silver_dim_banco_mundial') }}
),

-- Coleta o último ano disponível para cada KPI para cada país
bm_yoy as (
    select
        nome_pais,
        -- Usamos a função MAX com FILTER para encontrar o ano mais recente com valor não nulo
        -- para cada métrica específica, garantindo que não peguemos dados desatualizados.
        max(ano) filter (where pib_usd is not null) as ano_pib,
        max(ano) filter (where renda_per_capita is not null) as ano_renda,
        max(ano) filter (where inflacao is not null) as ano_inflacao,
        max(ano) filter (where crescimento_pib is not null) as ano_crescimento_pib,
        max(ano) filter (where invest_estrangeiro is not null) as ano_invest_estrangeiro,
        max(ano) filter (where florestas_percentual is not null) as ano_florestas,
        max(ano) filter (where energia_per_capita is not null) as ano_energia,
        max(ano) filter (where gasto_militar_pib is not null) as ano_gasto_militar,
        max(ano) filter (where taxa_homicidios_100mil is not null) as ano_homicidios,
        max(ano) filter (where desemprego_percentual is not null) as ano_desemprego,
        max(ano) filter (where expectativa_vida is not null) as ano_expectativa,
        max(ano) filter (where gasto_saude_pib is not null) as ano_gasto_saude
    from bm
    group by nome_pais
),

-- Adiciona os valores dos KPIs e o cálculo do ano a ano (YoY) do ano mais recente
bm_final as (
    select
        t1.nome_pais,
        -- Busca o ano e o valor do PIB para o ano mais recente encontrado acima.
        t1.ano as ano_pib,
        t1.pib_usd,
        -- Calcula a variação anual (Year-over-Year - YoY) do PIB.
        round(((t1.pib_usd - lag(t1.pib_usd) over(partition by t1.nome_pais order by t1.ano)) / nullif(lag(t1.pib_usd) over(partition by t1.nome_pais order by t1.ano),0)) * 100, 2) as pib_yoy,
        
        -- Repete a mesma lógica para os demais KPIs
        t2.ano as ano_renda,
        t2.renda_per_capita,
        round(((t2.renda_per_capita - lag(t2.renda_per_capita) over(partition by t2.nome_pais order by t2.ano)) / nullif(lag(t2.renda_per_capita) over(partition by t2.nome_pais order by t2.ano),0)) * 100, 2) as renda_per_capita_yoy,
        
        t3.ano as ano_inflacao,
        t3.inflacao,
        round(((t3.inflacao - lag(t3.inflacao) over(partition by t3.nome_pais order by t3.ano)) / nullif(lag(t3.inflacao) over(partition by t3.nome_pais order by t3.ano),0)) * 100, 2) as inflacao_yoy,
        
        t4.ano as ano_crescimento,
        t4.crescimento_pib,
        round(((t4.crescimento_pib - lag(t4.crescimento_pib) over(partition by t4.nome_pais order by t4.ano)) / nullif(lag(t4.crescimento_pib) over(partition by t4.nome_pais order by t4.ano),0)) * 100, 2) as crescimento_pib_yoy,
        
        t5.ano as ano_investimento,
        t5.invest_estrangeiro,
        round(((t5.invest_estrangeiro - lag(t5.invest_estrangeiro) over(partition by t5.nome_pais order by t5.ano)) / nullif(lag(t5.invest_estrangeiro) over(partition by t5.nome_pais order by t5.ano),0)) * 100, 2) as invest_estrangeiro_yoy,
        
        t6.ano as ano_florestas,
        t6.florestas_percentual,
        round(((t6.florestas_percentual - lag(t6.florestas_percentual) over(partition by t6.nome_pais order by t6.ano)) / nullif(lag(t6.florestas_percentual) over(partition by t6.nome_pais order by t6.ano),0)) * 100, 2) as florestas_percentual_yoy,
        
        t7.ano as ano_energia,
        t7.energia_per_capita,
        round(((t7.energia_per_capita - lag(t7.energia_per_capita) over(partition by t7.nome_pais order by t7.ano)) / nullif(lag(t7.energia_per_capita) over(partition by t7.nome_pais order by t7.ano),0)) * 100, 2) as energia_per_capita_yoy,
        
        t8.ano as ano_gasto_militar,
        t8.gasto_militar_pib,
        round(((t8.gasto_militar_pib - lag(t8.gasto_militar_pib) over(partition by t8.nome_pais order by t8.ano)) / nullif(lag(t8.gasto_militar_pib) over(partition by t8.nome_pais order by t8.ano),0)) * 100, 2) as gasto_militar_pib_yoy,
        
        t9.ano as ano_homicidios,
        t9.taxa_homicidios_100mil,
        round(((t9.taxa_homicidios_100mil - lag(t9.taxa_homicidios_100mil) over(partition by t9.nome_pais order by t9.ano)) / nullif(lag(t9.taxa_homicidios_100mil) over(partition by t9.nome_pais order by t9.ano),0)) * 100, 2) as taxa_homicidios_100mil_yoy,
        
        t10.ano as ano_desemprego,
        t10.desemprego_percentual,
        round(((t10.desemprego_percentual - lag(t10.desemprego_percentual) over(partition by t10.nome_pais order by t10.ano)) / nullif(lag(t10.desemprego_percentual) over(partition by t10.nome_pais order by t10.ano),0)) * 100, 2) as desemprego_percentual_yoy,
        
        t11.ano as ano_expectativa,
        t11.expectativa_vida,
        round(((t11.expectativa_vida - lag(t11.expectativa_vida) over(partition by t11.nome_pais order by t11.ano)) / nullif(lag(t11.expectativa_vida) over(partition by t11.nome_pais order by t11.ano),0)) * 100, 2) as expectativa_vida_yoy,
        
        t12.ano as ano_gasto_saude,
        t12.gasto_saude_pib,
        round(((t12.gasto_saude_pib - lag(t12.gasto_saude_pib) over(partition by t12.nome_pais order by t12.ano)) / nullif(lag(t12.gasto_saude_pib) over(partition by t12.nome_pais order by t12.ano),0)) * 100, 2) as gasto_saude_pib_yoy
    from bm_yoy
    --  LEFT JOINs para buscar o valor de cada KPI no seu ano mais recente.
      left join bm t1 on bm_yoy.nome_pais = t1.nome_pais and bm_yoy.ano_pib = t1.ano
    left join bm t2 on bm_yoy.nome_pais = t2.nome_pais and bm_yoy.ano_renda = t2.ano
    left join bm t3 on bm_yoy.nome_pais = t3.nome_pais and bm_yoy.ano_inflacao = t3.ano
    left join bm t4 on bm_yoy.nome_pais = t4.nome_pais and bm_yoy.ano_crescimento_pib = t4.ano
    left join bm t5 on bm_yoy.nome_pais = t5.nome_pais and bm_yoy.ano_invest_estrangeiro = t5.ano
    left join bm t6 on bm_yoy.nome_pais = t6.nome_pais and bm_yoy.ano_florestas = t6.ano
    left join bm t7 on bm_yoy.nome_pais = t7.nome_pais and bm_yoy.ano_energia = t7.ano
    left join bm t8 on bm_yoy.nome_pais = t8.nome_pais and bm_yoy.ano_gasto_militar = t8.ano
    left join bm t9 on bm_yoy.nome_pais = t9.nome_pais and bm_yoy.ano_homicidios = t9.ano
    left join bm t10 on bm_yoy.nome_pais = t10.nome_pais and bm_yoy.ano_desemprego = t10.ano
    left join bm t11 on bm_yoy.nome_pais = t11.nome_pais and bm_yoy.ano_expectativa = t11.ano
    left join bm t12 on bm_yoy.nome_pais = t12.nome_pais and bm_yoy.ano_gasto_saude = t12.ano
)

-- União final das tabelas para criar a camada Gold
select
    -- Seleciona dados de fatos sobre o país.
    f.nome_pais,
    f.capital,
    f.regiao,
    f.subregiao,
    f.populacao,
    f.area_km2,
    f.idiomas,
    f.moedas,
    f.bandeira_url,

    -- Seleciona códigos de identificação do país.
    p.codigo_alpha2,
    p.codigo_alpha3,
    p.codigo_numerico,
    p.codigo_olimpico,

    -- Seleciona os dados climáticos.
    c.temperatura,
    c.sensacao_termica,
    c.umidade,
    c.descricao_clima,

    -- Seleciona os KPIs do Banco Mundial com os valores e variação mais recentes.
    y.ano_pib,
    y.pib_usd,
    y.pib_yoy,
    y.ano_renda,
    y.renda_per_capita,
    y.renda_per_capita_yoy,
    y.ano_inflacao,
    y.inflacao,
    y.inflacao_yoy,
    y.ano_crescimento,
    y.crescimento_pib,
    y.crescimento_pib_yoy,
    y.ano_investimento,
    y.invest_estrangeiro,
    y.invest_estrangeiro_yoy,
    y.ano_florestas,
    y.florestas_percentual,
    y.florestas_percentual_yoy,
    y.ano_energia,
    y.energia_per_capita,
    y.energia_per_capita_yoy,
    y.ano_gasto_militar,
    y.gasto_militar_pib,
    y.gasto_militar_pib_yoy,
    y.ano_homicidios,
    y.taxa_homicidios_100mil,
    y.taxa_homicidios_100mil_yoy,
    y.ano_desemprego,
    y.desemprego_percentual,
    y.desemprego_percentual_yoy,
    y.ano_expectativa,
    y.expectativa_vida,
    y.expectativa_vida_yoy,
    y.ano_gasto_saude,
    y.gasto_saude_pib,
    y.gasto_saude_pib_yoy
from fato f
left join dim_pais p
    on f.nome_oficial = p.nome_oficial
left join dim_clima c
    on f.nome_resumido = c.codigo_pais
left join bm_final y
    on f.nome_pais = y.nome_pais
with bm as (
    -- Seleciona os dados do Banco Mundial já tratados
    select
        nome_pais,
        ano,
        pib_usd,
        renda_per_capita
    from {{ ref('silver_dim_banco_mundial') }}
),

kpi_mais_recente as (
    -- Filtra o ano mais recente disponível para cada KPI por país
    select
        nome_pais,
        pib_usd,
        renda_per_capita,
        row_number() over (partition by nome_pais order by ano desc) as rn
    from bm
    where pib_usd is not null and renda_per_capita is not null
)

select
    -- Informações do país (corrigidas para 'f')
    k.nome_pais,
    f.regiao,
    f.subregiao,
    f.populacao,

    -- KPIs mais recentes
    k.pib_usd,
    k.renda_per_capita,

    -- Ranking por PIB
    rank() over (order by k.pib_usd desc) as rank_pib,
    
    -- Ranking por Renda Per Capita
    rank() over (order by k.renda_per_capita desc) as rank_renda_per_capita
    
from kpi_mais_recente k
left join {{ ref('silver_fato_pais') }} f on k.nome_pais = f.nome_pais
left join {{ ref('silver_dim_pais') }} p on k.nome_pais = p.nome_pais
where k.rn = 1
order by rank_pib asc
{{ config(
    materialized='table'
) }}

-- União da tabela de fatos (KPIs mais recentes) com a tabela de dimensão (atributos do país)
select
    f.ano,
    f.categoria_renda,
    f.nivel_inflacao,
    f.crescimento_pib_categoria,
    f.nivel_violencia,
    f.cobertura_florestal,
    f.categoria_expectativa_vida,
    
    d.nome_pais,
    d.regiao,
    d.subregiao,
    d.densidade_populacional_categoria,
    d.tamanho_pais_categoria,
    d.temperatura,
    d.umidade
    
from {{ ref('gold_fatos_kpis_mais_recente') }} f
left join {{ ref('gold_dim_paises_analitica') }} d
    on f.nome_pais = d.nome_pais
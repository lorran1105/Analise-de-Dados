{{ config(
    materialized='ephemeral'
) }}

with bronze as (

    select * from {{ ref('bronze_clima_capital') }}

),

tratado as (

    select
        -- Padronização de identificadores
        upper(trim(cidade)) as cidade,
        upper(trim(codigo_pais)) as codigo_pais,

        -- Métricas convertidas
        cast(temperatura as numeric) as temperatura,
        cast(sensacao_termica as numeric) as sensacao_termica,
        cast(temperatura_min as numeric) as temperatura_min,
        cast(temperatura_max as numeric) as temperatura_max,
        cast(umidade as int) as umidade,

        -- Descrição padronizada
        initcap(
            regexp_replace(trim(descricao_clima), '\s+', ' ', 'g')
        ) as descricao_clima,

        -- Horário convertido corretamente para timestamp
        to_timestamp(horario_medicao, 'DD/MM/YYYY HH24:MI:SS') as horario_medicao,

        -- Marca o último registro por país
        row_number() over (
            partition by codigo_pais
            order by to_timestamp(horario_medicao, 'DD/MM/YYYY HH24:MI:SS') desc
        ) as rn
    from bronze
)

-- Mantém apenas o registro mais recente por país
select *
from tratado
where rn = 1
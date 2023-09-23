WITH
    base as (
        select DISTINCT
            vacina_nome
            , dose
        from {{ ref('stg_APIdatasus_geral') }}
    ),
    transformacoes as (
        select
            row_number() over (order by vacina_nome, dose ) as vacina_sk
            , vacina_nome
            , dose
        from base
    )
select *
from transformacoes

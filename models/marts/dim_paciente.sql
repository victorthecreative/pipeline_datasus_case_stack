WITH
    base as (
        select *
        from {{ ref('stg_APIdatasus_geral') }}
    ),
    transformacoes as (
        select
            row_number() over (order by paciente_id) as paciente_sk
            , paciente_id
            , idade
            , sexo
            , raca_id
            , raca_descricao
        from base
    )
select *
from transformacoes
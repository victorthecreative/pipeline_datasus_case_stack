with
    base as (
        select distinct
            municipio,
            estado
        from {{ ref('stg_APIdatasus_geral') }}
    ),
    transformacoes as (
        select
            row_number() over (order by municipio, estado) as municipio_estado_sk,
            municipio,
            estado
        from base
    )
select *
from transformacoes

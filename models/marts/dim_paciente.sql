{{
    config(
        materialized='table'
        , cluster_by=[
            'paciente_id'
            ,'raca_id'
            , 'sexo'
        ]
    )
}}

with
    base as (
        select distinct
            paciente_id
            , idade
            , sexo
            , raca_id
            , raca_descricao
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

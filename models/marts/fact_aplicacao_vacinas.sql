{{
    config(
        materialized='table'
        , partition_by={
            'data_aplicacao': 'date'
        }
        , cluster_by=[
            'sk_paciente'
            , 'sk_endereco'
            , 'sk_vacina'
        ]
    )
}}



with
    pacientes as (
        select *
        from {{ ref('dim_paciente') }}
    ),
    endereco as (
        select *
        from {{ ref('dim_endereco') }}
    ),
    vacina as (
        select *
        from {{ ref('dim_vacina') }}
    ),
    aplicacao as (
        select *
        from {{ ref('stg_APIdatasus_geral') }}
    ),
    joined_tabelas as (
        select
            aplicacao.data_aplicacao,
            aplicacao.ano_aplicacao,
            aplicacao.mes_aplicacao,
            pacientes.paciente_sk as sk_paciente,
            endereco.municipio_estado_sk as sk_endereco,
            vacina.vacina_sk as sk_vacina
        from aplicacao
        LEFT JOIN pacientes on aplicacao.paciente_id = pacientes.paciente_id
        LEFT JOIN endereco on aplicacao.municipio = endereco.municipio and aplicacao.estado = endereco.estado
        LEFT JOIN vacina on aplicacao.vacina_nome = vacina.vacina_nome and aplicacao.dose = vacina.dose
    ),
    transformacoes as (
        select
            {{ dbt_utils.generate_surrogate_key(['fk_paciente', 'data_aplicacao', 'fk_vacina']) }} as sk_aplicacao,
            *
        from joined_tabelas
    )
select *
from transformacoes

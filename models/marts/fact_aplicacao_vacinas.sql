WITH
    pacientes as (
        SELECT *
        FROM {{ ref('dim_paciente') }}
    ),
    endereco as (
        SELECT *
        FROM {{ ref('dim_endereco') }}
    ),
    vacina as (
        SELECT *
        FROM {{ ref('dim_vacina') }}
    ),
    aplicacao as (
        SELECT *
        FROM {{ ref('stg_APIdatasus_geral') }}
    ),
    joined_tabelas as (
        SELECT
            aplicacao.data_aplicacao,
            aplicacao.ano_aplicacao,
            aplicacao.mes_aplicacao,
            pacientes.paciente_sk as fk_paciente,
            endereco.municipio_estado_sk as fk_endereco,
            vacina.vacina_sk as fk_vacina
        FROM aplicacao
        LEFT JOIN pacientes ON aplicacao.paciente_id = pacientes.paciente_id
        LEFT JOIN endereco ON aplicacao.municipio = endereco.municipio AND aplicacao.estado = endereco.estado
        LEFT JOIN vacina ON aplicacao.vacina_nome = vacina.vacina_nome AND aplicacao.dose = vacina.dose
    ),
    transformacoes as (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['fk_paciente', 'data_aplicacao', 'fk_vacina']) }} as sk_aplicacao,
            *
        FROM joined_tabelas
    )
SELECT *
FROM transformacoes

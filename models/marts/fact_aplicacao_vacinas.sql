with
    fato as (
        select
            dim_pacientes.paciente_sk,
            dim_endereco.municipio_estado_sk,
            dim_vacina.vacina_sk,
            stg_APIdatasus_geral.data_aplicacao,
            stg_APIdatasus_geral.ano_aplicacao,
            stg_APIdatasus_geral.mes_aplicacao
        from {{ ref('stg_APIdatasus_geral') }} as stg_APIdatasus_geral
        join {{ ref('dim_pacientes') }} as dim_pacientes on stg_APIdatasus_geral.paciente_id = dim_pacientes.paciente_id
        join {{ ref('dim_endereco') }} as dim_endereco on stg_APIdatasus_geral.municipio = dim_endereco.municipio AND stg_APIdatasus_geral.estado = dim_endereco.estado
        join {{ ref('dim_vacina') }} as dim_vacina on stg_APIdatasus_geral.vacina_nome = dim_vacina.vacina_nome AND stg_APIdatasus_geral.dose = dim_vacina.dose
    )
select *
from fato

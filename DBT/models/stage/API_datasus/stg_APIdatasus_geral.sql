
WITH
    fonte_datasus AS (
        SELECT *
        FROM {{ source('API_datasus', 'datasus_vacinacao_COVID19') }} 
    ),
    renomear AS (
        SELECT
              CAST(paciente_id AS STRING) AS paciente_id
            , CAST(paciente_idade AS INT64) AS idade	
            , CAST(paciente_enumSexoBiologico AS STRING) AS sexo	
            , CAST(paciente_racaCor_codigo AS INT64) AS raca_id
            , CAST(paciente_racaCor_valor AS STRING) AS raca_descricao	
            , CAST(paciente_endereco_nmMunicipio AS STRING) AS municipio
            , CAST(paciente_endereco_uf AS STRING) AS estado
            , REGEXP_EXTRACT(CAST(vacina_nome AS STRING), r'COVID-19(.*)') AS vacina_nome
            , CASE 
                WHEN vacina_descricao_dose LIKE '1ª Dose' THEN 1
                WHEN vacina_descricao_dose LIKE '2ª Dose' THEN 2
                WHEN vacina_descricao_dose LIKE '3ª Dose' THEN 3
                WHEN vacina_descricao_dose LIKE '4ª Dose' THEN 4
                ELSE NULL
              END AS dose
            , CAST(vacina_dataAplicacao AS DATE) AS data_aplicacao
            , EXTRACT(YEAR FROM CAST(vacina_dataAplicacao AS DATE)) AS ano_aplicacao
            , EXTRACT(MONTH FROM CAST(vacina_dataAplicacao AS DATE)) AS mes_aplicacao
        FROM fonte_datasus
    )

SELECT *
FROM renomear

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv
from io import StringIO
from datetime import datetime

# Define the transformation function
def transform_function(line):
    csv_file = StringIO(line)
    reader = csv.DictReader(csv_file, fieldnames=[
        'paciente_id',
        'paciente_idade',
        'paciente_enumSexoBiologico',
        'paciente_racaCor_codigo',
        'paciente_racaCor_valor',
        'paciente_endereco_nmMunicipio',
        'paciente_endereco_uf',
        'vacina_nome',
        'vacina_descricao_dose',
        'vacina_dataAplicacao',
    ])
    row = next(reader)
    if row['vacina_dataAplicacao']:
        date_object = datetime.strptime(row['vacina_dataAplicacao'], '%Y-%m-%dT%H:%M:%S.%fZ')
        row['vacina_dataAplicacao'] = date_object.strftime('%Y-%m-%d')
    return row

# Define pipeline options
pipeline_options = PipelineOptions(
    project='datasus-pipeline',
    runner='DataflowRunner',
    job_name='write-bigquery',
    region='us-central1',
    staging_location='gs://datasus-case-stack/temp',
    temp_location='gs://datasus-case-stack/temp',
    template_location='gs://datasus-case-stack/template_location/job_write_bigquery'
)

# Define table schema
table_schema = {
    'fields': [
        {'name': 'paciente_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'paciente_idade', 'type': 'INTEGER'},
        {'name': 'paciente_enumSexoBiologico', 'type': 'STRING'},
        {'name': 'paciente_racaCor_codigo', 'type': 'INTEGER'},
        {'name': 'paciente_racaCor_valor', 'type': 'STRING'},
        {'name': 'paciente_endereco_nmMunicipio', 'type': 'STRING'},
        {'name': 'paciente_endereco_uf', 'type': 'STRING'},
        {'name': 'vacina_nome', 'type': 'STRING'},
        {'name': 'vacina_descricao_dose', 'type': 'STRING'},
        {'name': 'vacina_dataAplicacao', 'type': 'DATE'},
    ]
}

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    (
        pipeline
        | 'Read from Cloud Storage' >> beam.io.ReadFromText('gs://datasus-case-stack/vacina_datasus.csv')
        | 'Transform Data' >> beam.Map(lambda x: transform_function(x))
        | 'Write to BigQuery' >> WriteToBigQuery(
            table='covid19_table_stg',
            dataset='datasus_vacinacao_covid19_stg',
            project='datasus-pipeline',
            schema=table_schema,
        )
    )
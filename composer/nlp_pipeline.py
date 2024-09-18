from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryGetDatasetTablesOperator
# from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryCreateDataTransferOperator
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
# from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
# from airflow.providers.google.cloud.operators.gcs import
import pendulum
import datetime

import logging


@dag(schedule_interval=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"))
def nlp_pipeline():
    HPO_ID = Variable.get('hpo_id')
    PROJECT_ID = Variable.get('project_id')
    SUBNETWORK = Variable.get('subnetwork')
    NOTES_BUCKET_NAME = Variable.get('notes_bucket_name')
    CLAMP_BUCKET_NAME = Variable.get('clamp_bucket_name')
    DATAFLOW_BUCKET_NAME = Variable.get('dataflow_bucket_name')
    CLAMP_JAR_NAME = Variable.get('clamp_jar_name')
    NOTES_BQ_DATASET = Variable.get('notes_bq_dataset')
    NOTE_NLP_BQ_DATASET = Variable.get('note_nlp_bq_dataset')

    @task()
    def export_notes_to_storage():
        export_query = f"""
            #standardSQL
            EXPORT DATA
            OPTIONS (
                uri='gs://{NOTES_BUCKET_NAME}/{HPO_ID}/input/*.jsonl',
                overwrite=true,
                format='JSON'
            ) AS
            SELECT *
            FROM `{PROJECT_ID}.{NOTES_BQ_DATASET}.{HPO_ID}_note`
            WHERE note_id NOT IN (SELECT note_id
                FROM `{PROJECT_ID}.{NOTES_BQ_DATASET}.{HPO_ID}_note_nlp`
            )
            LIMIT 50

        """.strip()

        hook = bigquery.BigQueryHook(gcp_conn_id='google_cloud_default',
                                     use_legacy_sql=False)

        logging.info(
            f'Exporting {HPO_ID} notes to gs://{NOTES_BUCKET_NAME}/{HPO_ID}/input/*.jsonl.'
        )
        logging.info(export_query)
        # hook.run_query(export_query)
        hook.insert_job(
            configuration={
                'query': {
                    'query': export_query,
                    'useLegacySQL': False,
                    # 'timeoutMs': 1
                }
            },
            # job_id='export_notes'
        )
        logging.info(f'Export complete.')

    jar_path = f"gs://{CLAMP_BUCKET_NAME}/{CLAMP_JAR_NAME}"

    run_clamp = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id='clamp-job',
        jar=jar_path,
        job_class='org.allofus.curation.pipeline.CurationNLPMain',
        dataflow_config={
            'job_name': '{{task.task_id}}',
            'wait_until_finished': True
        },
        pipeline_options={
            'gcpTempLocation': f"gs://{DATAFLOW_BUCKET_NAME}/gcp_tmp",
            'stagingLocation': f"gs://{DATAFLOW_BUCKET_NAME}/staging",
            'tempLocation': f"gs://{DATAFLOW_BUCKET_NAME}/tmp",
            'project': PROJECT_ID,
            'usePublicIps': False,
            'region': 'us-central1',
            'numWorkers': 10,  # Use 10
            'maxNumWorkers': 10,  # Use 10
            'numberOfWorkerHarnessThreads': 2,
            'workerMachineType': 'custom-8-16384',
            'diskSizeGb': 50,
            'experiments': 'use_runner_v2',
            'subnetwork': SUBNETWORK,
            'input': f"gs://{NOTES_BUCKET_NAME}/{HPO_ID}/input/",
            'output': f"gs://{NOTES_BUCKET_NAME}/{HPO_ID}/output/",
            'pipeline': 'clinical_pipeline.pipeline.jar',
            'maxClampThreads': 10,
            'maxOutputPartitionSeconds': 1800,
            'maxOutputBatchSize': 100000,
            'resourcesDir': f"gs://{DATAFLOW_BUCKET_NAME}/resources",
            'inputType': 'jsonl',
            'outputType': 'jsonl',
            # 'streaming': True,
            # 'enableStreamingEngine': True
        })

    @task()
    def import_note_nlp_to_bigquery():
        import_query = f"""
            #standardSQL
            LOAD DATA INTO `{PROJECT_ID}.{NOTE_NLP_BQ_DATASET}.{HPO_ID}_note_nlp` (
            note_nlp_id INT64,
            note_id INT64,
            section_concept_id INT64,
            snippet STRING,
            offset STRING,
            lexical_variant STRING,
            note_nlp_concept_id INT64,
            note_nlp_source_concept_id INT64,
            nlp_system STRING,
            nlp_date DATE,
            nlp_datetime TIMESTAMP,
            term_exists STRING,
            term_temporal STRING,
            term_modifiers STRING
            )
            FROM FILES (
            format='JSON',
            uris = ['gs://{NOTES_BUCKET_NAME}/{HPO_ID}/output/*']
            )
        """

        hook = bigquery.BigQueryHook(gcp_conn_id='google_cloud_default',
                                     use_legacy_sql=False)

        logging.info(
            f'Importing {HPO_ID} note_nlp records to {PROJECT_ID}.{NOTE_NLP_BQ_DATASET}.{HPO_ID}_note_nlp.'
        )
        # hook.run_query(import_query)

        hook.insert_job(configuration={
            'query': {
                'query': import_query,
                'useLegacySQL': False,
                'timeoutMs': 60000
            }
        },
                        # job_id='import_note_nlp'
                        )

        logging.info(f'Import complete.')

    export_notes_to_storage() >> run_clamp >> import_note_nlp_to_bigquery()


nlp_pipeline_dag = nlp_pipeline()

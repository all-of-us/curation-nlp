from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.google.cloud.hooks import bigquery
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryCreateDataTransferOperator
# from airflow.providers.google.cloud.operators.gcs import
import pendulum

import logging


@dag(schedule_interval=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"))
def nlp_pipeline():
    HPO_ID = Variable.get('hpo_id')
    PROJECT_ID = Variable.get('project_id')
    BUCKET_NAME = Variable.get('bucket_name')
    NOTES_BQ_DATASET = Variable.get('input_bq_dataset')

    # @task()
    # def import_notes():
    #     # a transfer run using a specialized Airflow operator
    #     transfer_config = {
    #         "destination_dataset_id": DATASET_NAME,
    #         "display_name": "test data transfer",
    #         "data_source_id": "google_cloud_storage",
    #         "schedule_options": {"disable_auto_scheduling": True},
    #         "params": {
    #             "field_delimiter": ",",
    #             "max_bad_records": "0",
    #             "skip_leading_rows": "1",
    #             "data_path_template": BUCKET_URI,
    #             "destination_table_name_template": DTS_BQ_TABLE,
    #             "file_format": "CSV",
    #         },
    #     }

    @task()
    def export_notes_to_storage():
        export_query_tmpl = """
            EXPORT DATA
            OPTIONS (
                uri='gs:{bucket_name}/{hpo_id}/input/*.jsonl',
                overwrite=true,
                format='JSON'
            ) AS
            SELECT DISTINCT *
            FROM `{project_id}.{dataset_id}.{hpo_id}_note`
            WHERE note_id NOT IN (SELECT note_id
                FROM `{project_id}.{dataset_id}.{hpo_id}_note_nlp`
            )
        
        """

        export_query = export_query_tmpl.format(bucket_name=BUCKET_NAME,
                                                hpo_id=HPO_ID,
                                                project_id=PROJECT_ID,
                                                dataset_id=NOTES_BQ_DATASET)

        hook = bigquery.BigQueryHook(gcp_conn_id='clamp_prod',
                                     use_legacy_sql=False)

        logging.info(f'Exporting {HPO_ID} notes to {BUCKET_NAME}.')
        hook.run_query(export_query)
        logging.info(f'Export complete.')

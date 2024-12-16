from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.models import Variable
# from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryCreateDataTransferOperator
# from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
# from airflow.providers.google.cloud.operators.gcs import
import pendulum
import datetime

import logging
from pathlib import Path
import json

DAGS_DIR = "/home/airflow/gcs/dags"
DBT_PROJECT_DIR = "/home/airflow/gcs/data/cleaning_rules"
DBT_MODELS_PATH = f"{DBT_PROJECT_DIR}/models"
MANIFEST_PATH = f"{DBT_PROJECT_DIR}/target/manifest.json"

with open(MANIFEST_PATH, 'r') as f:
    manifest = json.load(f)
    nodes = manifest['nodes']


@dag(schedule_interval=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"))
def cleaning_rules():

    dbt_dataset_id = Variable.get("DBT_DATASET_ID")

    @task.bash
    def clear_snapshot_cache():
        return f"bash {DAGS_DIR}/scripts/delete_snapshots.sh {dbt_dataset_id} "

    @task_group
    def run_cleaning_rules():
        cleaning_rule_tasks = {}
        for node_id, node_info in nodes.items():
            if node_info['resource_type'] == 'model' and not node_info[
                    'path'].startswith('post_snapshot'):
                cleaning_rule_tasks[node_id] = BashOperator(
                    task_id=f"{node_info['resource_type']}_{node_info['name']}",
                    bash_command=
                    f'dbt run --project-dir {DBT_PROJECT_DIR} --vars \'{{ "note_nlp_table_name": "{Variable.get("note_nlp_table_name")}" }}\' '
                    + f"--select {node_info['name']}")

        for node_id, node_info in nodes.items():
            if node_id in list(cleaning_rule_tasks.keys()):
                upstream_nodes = [
                    n for n in node_info['depends_on']['nodes']
                    if n in list(cleaning_rule_tasks.keys())
                ]
                if upstream_nodes:
                    for upstream_node in upstream_nodes:
                        cleaning_rule_tasks[
                            upstream_node] >> cleaning_rule_tasks[node_id]

    # @task.bash
    # def run_cleaning_rules():
    #     return f"dbt run --project-dir {DBT_PROJECT_DIR} --exclude 'post_snapshot'"

    @task.bash
    def run_first_snapshot():
        return f"dbt snapshot --project-dir {DBT_PROJECT_DIR}"

    @task.bash
    def progress_models():
        return f"bash {DAGS_DIR}/scripts/progress_models.sh {dbt_dataset_id} "

    @task.bash
    def run_second_snapshot():
        return f"dbt snapshot --project-dir {DBT_PROJECT_DIR}"

    @task.bash
    def post_snapshot():
        return f'dbt run --project-dir {DBT_PROJECT_DIR} --vars \'{{ "note_nlp_table_name": "{Variable.get("note_nlp_table_name")}" }}\' --select post_snapshot'

    clear_snapshot_cache() >> run_cleaning_rules() >> run_first_snapshot(
    ) >> progress_models() >> run_second_snapshot() >> post_snapshot()


cleaning_rule_dag = cleaning_rules()

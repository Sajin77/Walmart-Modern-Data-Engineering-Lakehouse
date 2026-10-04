import os
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from utils.databricks_helpers import trigger_and_monitor_databricks_job

# from airflow_dbt_project.dags.utils.databricks_helpers import trigger_and_monitor_databricks_job
# docker and airflow looks directly inside dags folder, so we need to use relative import to access the databricks_helpers.py file in the utils folder.


@dag(
        dag_id="orchestrate",
        schedule=None,
        catchup=False
)
def orchestrate():

    @task
    def ingest_cdc():
        db_host = os.getenv("DATABRICKS_HOST")
        db_token = os.getenv("DATABRICKS_TOKEN")
        db_job_id = os.getenv("DATABRICKS_WALMART_JOB_ID")
        result = trigger_and_monitor_databricks_job(host=db_host, token=db_token, job_id=db_job_id)
        return result

    @task.bash  # When using task.bash, task id is automatically generated based on the function name
    def clean_target():  # When using task.bash, the return value is treated as a bash command to execute
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running the dbt command
        return "cd /opt/airflow/walmart_project && dbt source freshness"
    

    @task.bash  # task.bash is the modern way to define a bash task in airflow 2.5+.
    def silver_technical():
        return "cd /opt/airflow/walmart_project && dbt run --select silver_t"

    @task.bash
    def silver_technical_tests():
        return "cd /opt/airflow/walmart_project && dbt test --select silver_t"

    @task.bash
    def silver_business():
        return "cd /opt/airflow/walmart_project && dbt run --select silver_b"

    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_b'
    )

    gold_ephermeral = BashOperator(
        task_id='gold_ephermeral',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/ephermeral'
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select gold/fact'
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical() >> silver_technical_tests() >> silver_business() >> silver_business_tests >> gold_ephermeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()
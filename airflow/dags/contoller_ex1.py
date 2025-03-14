from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def _do_something():
    return "my-object-s3-aws"

dag = DAG(
    dag_id="example_trigger_controller_dag",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval="@once",
    tags=['example'],
)

task_1 = PythonOperator(task_id='previous_task_id',
                        python_callable=_do_something)

trigger = TriggerDagRunOperator(
    task_id="test_trigger_dagrun",
    trigger_dag_id="example_trigger_target_dag",
    conf={
        "s3_object":
        "{{ ti.xcom_pull(task_ids='previous_task_id', key='return_value') }}"},
    dag=dag,
)

task_1 >> trigger

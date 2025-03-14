from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="example_trigger_target_dag",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
)


def run_this_func(**context):
    print("Remotely received value of {} ".format(
        context["dag_run"].conf["s3_object"]))


run_this = PythonOperator(
    task_id="run_this", python_callable=run_this_func, dag=dag)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: $s3_object"',
    env={'s3_object': '{{ dag_run.conf["s3_object"] if dag_run else "" }}'},
    dag=dag,
)

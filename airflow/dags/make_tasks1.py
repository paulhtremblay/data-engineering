from airflow.operators.dummy_operator import DummyOperator

def make_tasks(dag, names):
    task_ids = []
    for name in names:
        task_id = DummyOperator(task_id=f'task_{name}', dag = dag)
        task_ids.append(task_id)

    final_task_id = DummyOperator(task_id=f'final', dag = dag)
    final_task_id.set_upstream(task_ids)

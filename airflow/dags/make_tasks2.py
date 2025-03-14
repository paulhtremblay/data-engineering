from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def outer_func(*args, **kwargs):
    def inner_func(*args_i, **kwargs_i):
        print('in innner')
        print(f'kwargs are {kwargs}')
        return 1
    return inner_func

def make_tasks(dag, names):
    task_ids = []
    for name in names:
        task_id = PythonOperator(
             python_callable = outer_func(col1 = 'col1', col2 = 'col2'),
             provide_context=True,
             task_id = f'task_{name}',
             dag = dag,
                         )
        task_ids.append(task_id)


    final_task_id = DummyOperator(task_id=f'final', dag = dag)
    final_task_id.set_upstream(task_ids)

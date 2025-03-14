import pendulum
import os
import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


import pprint
pp = pprint.PrettyPrinter(indent=4)
import make_tasks2


# start params
OWNER = 'Henry'
DAG_ID = "template_ex2_v1"
SCHEDULE_INTERVAL = '@daily'
START_DATE = datetime.datetime(2025,1,1,0,0,0)
END_DATE = datetime.datetime(2025,1,2,0,0,0)
DEPENDS_ON_PAST = True
CATCHUP = True
WAIT_FOR_DOWNSTREAM = True
NUM_RETRIES = 5
RETRY_DELAY_MINUTES = 1
RETRY_EXPONENTIAL_BACKOFF = True
EXECUTION_TIMEOUT_HOURS = 5
DEBUG = True
DESCRIPTION = 'dag template example'
VERBOSE = True
# end params

default_args = {
            'owner': OWNER,
            'start_date': START_DATE,
            'end_date': END_DATE,
            'retries': NUM_RETRIES,
            'retry_delay': datetime.timedelta(minutes=1),
            'depends_on_past':DEPENDS_ON_PAST,
            'retry_exponential_backoff': RETRY_EXPONENTIAL_BACKOFF,
            'wait_for_downstream':WAIT_FOR_DOWNSTREAM,
            'execution_timeout':datetime.timedelta(hours=5)
            }

dag =  DAG(DAG_ID,
                default_args=default_args,
                schedule_interval = SCHEDULE_INTERVAL,
                description = DESCRIPTION,
                catchup = CATCHUP,
                )
    

make_tasks2.make_tasks(
        dag = dag,
        names = ['account', 'bom'],
        )
                                            

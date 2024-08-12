import os
import argparse
import pprint
import os
import concurrent
import csv

import sqlite3

from google.cloud import pubsub_v1
from google.cloud import bigquery



pp = pprint.PrettyPrinter(indent= 4)


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("subscription",  help="subscription")
    parser.add_argument('--timeout', type = int, default = 500)
    parser.add_argument("--bq",  action ='store_true', help = 'use_bq')  
    args = parser.parse_args()
    return args

def  _get_bq(uuid_):
    client = bigquery.Client()
    sql = """
    SELECT sku,  RAND() as n
     FROM
        `paul-henry-tremblay.data_engineering.ads_with_cluster`

     WHERE
       date = "2024-08-05"
       and  order_id = 4000
       and creative_id = 2000
       and placement_id = 2000
    """

    query = (sql)
    query_job = client.query(sql)  
    rows = query_job.result()  

    with open(f'data/{uuid_}.csv', 'w') as write_obj:
        csv_writer =  csv.writer(write_obj)
        for row in rows:
            r = [x[1] for x in row.items()]
            csv_writer.writerow(r)

def subscribe(
        subscription_id, 
        project_id = 'paul-henry-tremblay', 
        timeout = 5,
        use_bq = False,
        verbose = False):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def _to_db(message):
        con = sqlite3.connect('query_events')
        cur = con.cursor()
        cur.execute(f"""
        INSERT INTO events (event_id, status)
        VALUES ('{message.data.decode('utf8')}', 'done')
        """)
        con.commit()

    def callback_bq(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        _get_bq(uuid_ = message.data.decode('utf8'))
        _to_db(message)
        message.ack()

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        _to_db(message)
        message.ack()

    if use_bq:
        f = callback_bq
    else:
        f = callback

    streaming_pull_future = subscriber.subscribe(
            subscription_path, 
            callback=f)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  
            streaming_pull_future.result()  
        except concurrent.futures._base.TimeoutError:
            streaming_pull_future.cancel()  
            streaming_pull_future.result()  

if __name__== '__main__':
    args = _get_args()
    subscribe(
            subscription_id = args.subscription,
            verbose = args.verbose,
            timeout = args.timeout,
            use_bq = args.bq,
            )

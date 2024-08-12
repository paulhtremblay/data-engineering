import os
import time
import uuid
import datetime
import argparse
import csv

from google.cloud import pubsub_v1
import sqlite3

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("topic",  help="topic")
    parser.add_argument("--read-data", '-r',   
            action ='store_true', help = 'read data')  
    args = parser.parse_args()
    return args

def _init_db(db_name, table_name):
    if os.path.isfile(db_name):
        os.remove(db_name)
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.execute(f"""
    CREATE TABLE {table_name} (event_id INT, status TEXT)
    """)
    con.commit()

def request_with_no_work(num_requests = 5,
        project_id = 'paul-henry-tremblay',
        topic = 'api-pub-sub',
        db_name = 'query_events',
        table_name = 'events',
        init_db = False,
        read_data = False,
        verbose = False):
    n = 0
    time_results = []
    if init_db:
        _init_db(db_name, table_name = table_name)
    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/{project_id}/topics/{topic}'
    con = sqlite3.connect(db_name)
    while 1:
        n+= 1
        if verbose:
            print(f'working on number {n}')
        start = datetime.datetime.now()
        uuid_ = str(uuid.uuid1()).encode('utf8')
        future = publisher.publish(
                topic_name, uuid_)
        if verbose:
            print(f'pushed to {future.result()}')
        while 1:
            cur = con.cursor()
            res = cur.execute(f"""
            SELECT * FROM {table_name}
            WHERE event_id = '{uuid_.decode("utf8")}'
            """
            )
            exists = res.fetchone()
            if exists:
                for_web = []
                end = datetime.datetime.now()
                if read_data:
                    with open(f'data/{uuid_.decode("utf8")}.csv', 'r') as read_obj:
                        csv_reader = csv.reader(read_obj)
                        for counter, i in enumerate(csv_reader):
                            for_web.append(i)
                    if verbose:
                        print(f'num rows in {counter}')
                time_results.append((end - start).total_seconds())
                break

        if n == 5:
            break
    print(time_results)

def main(topic, read_data, verbose = False):
    request_with_no_work(
            topic = topic,
            read_data = read_data,
            verbose =verbose)


if __name__ == '__main__':
    args = _get_args()
    main(topic = args.topic,
            read_data = args.read_data,
            verbose = args.verbose
            )


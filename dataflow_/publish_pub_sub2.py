import warnings
import json
import random
import string
from google.cloud import pubsub_v1
import argparse

def _get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('topic', 
            help='topic name')
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    args = parser.parse_args()
    return args

"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable


publisher = pubsub_v1.PublisherClient()
publish_futures = []

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

def _make_data(len_ = 7):
    data = [
        {'serverID': 'server_1', 'CPU_Utilization': 0, 'timestamp': 1},
        {'serverID': 'server_2', 'CPU_Utilization': 10, 'timestamp': 1},
        {'serverID': 'server_3', 'CPU_Utilization': 20, 'timestamp': 3},
        {'serverID': 'server_1', 'CPU_Utilization': 30, 'timestamp': 2},
        {'serverID': 'server_2', 'CPU_Utilization': 40, 'timestamp': 3},
        {'serverID': 'server_3', 'CPU_Utilization': 50, 'timestamp': 6},
        {'serverID': 'server_1', 'CPU_Utilization': 60, 'timestamp': 7},
        {'serverID': 'server_2', 'CPU_Utilization': 70, 'timestamp': 7},
        {'serverID': 'server_3', 'CPU_Utilization': 80, 'timestamp': 14},
        {'serverID': 'server_2', 'CPU_Utilization': 85, 'timestamp': 8.5},
        {'serverID': 'server_1', 'CPU_Utilization': 90, 'timestamp': 14}
    ]


def my_publish(project_id, topic_id):
    topic_path = publisher.topic_path(project_id, topic_id)
    data = [
        {'serverID': 'server_1', 'CPU_Utilization': 0, 'timestamp': 1},
        {'serverID': 'server_2', 'CPU_Utilization': 10, 'timestamp': 1},
        {'serverID': 'server_3', 'CPU_Utilization': 20, 'timestamp': 3},
        {'serverID': 'server_1', 'CPU_Utilization': 30, 'timestamp': 2},
        {'serverID': 'server_2', 'CPU_Utilization': 40, 'timestamp': 3},
        {'serverID': 'server_3', 'CPU_Utilization': 50, 'timestamp': 6},
        {'serverID': 'server_1', 'CPU_Utilization': 60, 'timestamp': 7},
        {'serverID': 'server_2', 'CPU_Utilization': 70, 'timestamp': 7},
        {'serverID': 'server_3', 'CPU_Utilization': 80, 'timestamp': 14},
        {'serverID': 'server_2', 'CPU_Utilization': 85, 'timestamp': 8.5},
        {'serverID': 'server_1', 'CPU_Utilization': 90, 'timestamp': 14}
    ]

    for i in data:
        data_ = json.dumps(i).encode('utf8')
        publish_future = publisher.publish(topic_path, data_)
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")

if __name__ == '__main__':
    args = _get_args()
    my_publish(project_id = "paul-henry-tremblay", topic_id = args.topic)

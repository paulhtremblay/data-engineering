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

def _make_data_raw():
    data = {'key': 'value'}
    data = json.dumps(data).encode('utf8')
    return data

def _make_data(len_ = 7):
    d = {}
    for i in range(random.randint(1,5)):
        key = 'key ' + ''.join(
                random.choice(
                    string.ascii_uppercase + string.digits) for _ in range(len_))
        value = 'value ' + ''.join(
                random.choice(
                    string.ascii_uppercase + string.digits) for _ in range(len_))
        d[key] = value
    return json.dumps(d).encode('utf8')



def my_publish(project_id, topic_id, verbose = False):
    topic_path = publisher.topic_path(project_id, topic_id)

    for i in range(10):
        data = _make_data()
        if verbose:
            print(f'data is {data}')
        publish_future = publisher.publish(topic_path, data)
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")

if __name__ == '__main__':
    args = _get_args()
    my_publish(
            project_id = "paul-henry-tremblay", 
            topic_id = args.topic,
            verbose = args.verbose)

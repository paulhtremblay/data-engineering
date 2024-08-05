from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

import argparse

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('subscription', 
            help='subscription name name')
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    args = parser.parse_args()
    return args

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

def get(project_id, subscription_id, timeout = 5):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  
            streaming_pull_future.result()  

if __name__ == '__main__':
    args = _get_args()
    get(
        project_id = "paul-henry-tremblay", 
        subscription_id = args.subscription)

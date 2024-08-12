import os
import argparse
import pprint
import os
from google.cloud import pubsub_v1
import concurrent

pp = pprint.PrettyPrinter(indent= 4)


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("subscription",  help="subscription")
    args = parser.parse_args()
    return args

def subscribe(
        subscription_id, 
        project_id = 'paul-henry-tremblay', 
        timeout = 5,
        verbose = False):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
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
            verbose = args.verbose
            )

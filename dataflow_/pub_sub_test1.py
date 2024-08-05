import warnings
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1



def callback(message):
    print(f"Received {message}.")
    message.ack()


def my_subscribe(project_id, subscription_id, timeout = 60):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    print(f"Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()

def my_publish(project_id, topic_id):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):
        data = "Message number {}".format(n)
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(future.result())

    print(f"Published messages to {topic_path}.")

if __name__ == '__main__':
    my_publish(project_id = "ad-tech-test-204121", topic_id = 'my-topic')
    my_subscribe(project_id = "ad-tech-test-204121", subscription_id = 'my-sub')

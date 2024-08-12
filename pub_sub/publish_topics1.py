import os
import argparse
import pprint
import os
from google.cloud import pubsub_v1

pp = pprint.PrettyPrinter(indent= 4)


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("topic",  help="topic")
    args = parser.parse_args()

    return args

def publish(
        topic, 
        verbose = False,
        project_id = 'paul-henry-tremblay'):
    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/{project_id}/topics/{topic}'
    #publisher.create_topic(name=topic_name)
    future = publisher.publish(topic_name, b'My first message!', spam='eggs')
    #future = publisher.publish(topic_path, data)
    print(future.result())



if __name__== '__main__':
    args = _get_args()
    publish(
            topic = args.topic,
            verbose = args.verbose
            )

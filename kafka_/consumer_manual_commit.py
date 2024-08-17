from time import sleep
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps
from json import loads

def get_consumer(enable_auto_commit = False):
    consumer = KafkaConsumer(
        'numtest',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=enable_auto_commit,
         group_id='my-group',
         consumer_timeout_ms=1000 * 5,
         value_deserializer=lambda x: loads(x.decode('utf-8')))
    return consumer

def get_data_(consumer):
    while True:
        poll_messages = consumer.poll()
        for message in consumer:
            print(message.value)

def get_data(consumer):
    for message in consumer:
        message = message.value
        print(message)
        #consumer.commit
        consumer.commit_async()


def main():
    consumer = get_consumer()
    get_data(consumer = consumer)

if __name__ == '__main__':
    main()


from time import sleep
from kafka import KafkaProducer
from json import dumps
import  argparse

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("number", type = int,  help="number of messages")
    args = parser.parse_args()
    return args

def on_send_error(excp):
    print(excp)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def get_producer():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                acks = 'all'
                )
    return producer

def make_data(producer, number = 1000):
    for e in range(number):
        data = {'number' : e}
        producer.send('numtest', value=data).add_errback(on_send_error)
        sleep(5)

def main(number):
    producer = get_producer()
    make_data(producer = producer, number = number)

if __name__ == '__main__':
    args = _get_args()
    main(args.number)


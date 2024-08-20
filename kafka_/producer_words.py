from time import sleep
from json import dumps
import  argparse
import datetime
import time

from kafka import KafkaProducer

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("--continuous", '-c',  action ='store_true')  
    parser.add_argument("number", type = int,  help="number of messages")
    args = parser.parse_args()
    return args

def get_producer():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                acks = 'all'
                )
    return producer

def make_data_continuous(producer):
    words = ['tiger', 'bear']
    while True:
        for j in words:
            data = {'word' : j,
                    'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            producer.send('words', value=data)
            time.sleep(.5)

def make_data(producer, number ):
    words = ['tiger', 'bear']
    for i in range(number):
        for j in words:
            data = {'word' : j,
                    'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            producer.send('words', value=data)

def main(number, continuous):
    producer = get_producer()
    if continuous:
        make_data_continuous(producer)
    else:
        make_data(producer = producer, number = number)

if __name__ == '__main__':
    args = _get_args()
    main(args.number, continuous = args.continuous)


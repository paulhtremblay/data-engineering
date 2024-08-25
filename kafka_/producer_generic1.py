from time import sleep
from json import dumps
import  argparse
import datetime
import time
import avro.schema
from avro.io import DatumWriter
import io
from datetime import timezone

from avro.io import DatumWriter

from kafka import KafkaProducer

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  

    parser.add_argument("--serializer", 
            choices = ['avro','json'],  
            default = 'json',
            help="serializer")
    parser.add_argument("--topic-name", 
            type = str,  
            required = True,
            help="name of topic")
    parser.add_argument("--data", 
            choices = ['words','numbers'],  
            default = 'words',
            help="func")
    parser.add_argument("number", 
            type = int,  
            nargs = "?",
            help="number")
    parser.add_argument("--sleep-time", 
            type = int,  
            default = 1,
            help="sleep time")
    args = parser.parse_args()
    return args

def produce(args):
    if args.serializer == 'json':
        producer = get_producer_json()
    elif args.serializer == 'avro':
        producer = get_producer_avro()
    gen = None
    if args.data == 'words':
        func = make_words
    elif args.data == 'numbers':
        gen = num_gen()
        func = make_numbers
    if not args.number:
        while True:
            func(producer = producer,
                    topic_name = args.topic_name,
                    gen = gen)
            time.sleep(args.sleep_time)
    else:
        for i in range(args.number):
            func(producer = producer,
                    topic_name = args.topic_name,
                    gen = gen)
            time.sleep(args.sleep_time)


def make_words(producer, topic_name, *args, **kwargs):
    words = ['tiger', 'bear']
    x = datetime.datetime.now().astimezone()
    for j in words:
        data = {'word' : j,
                'timestamp': x}
        producer.send(topic_name, value=data)

def num_gen():
    for i in range(100000):
        yield i

def make_numbers(producer, topic_name, gen):
        data = {'number' : next(gen)}
        producer.send(topic_name, value=data)

def get_producer_json():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                acks = 'all'
                )
    return producer

def avro_serializer(the_dict):
    schema_path = "words.avsc"
    schema = avro.schema.parse(open(schema_path).read())
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(the_dict, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes


def get_producer_avro():

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=avro_serializer,
                acks = 'all'
                )
    return producer

if __name__ == '__main__':
    args = _get_args()
    produce(args)


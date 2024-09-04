from time import sleep
from json import dumps
import json
import  argparse
import datetime
import time
import avro.schema
from avro.io import DatumWriter
import io
from datetime import timezone
import random

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
    parser.add_argument("--key", 
            type = str,  
            required = False,
            default = 'key1',
            help="name of key")
    parser.add_argument("--data", 
            choices = ['words','numbers', 'person'],  
            default = 'words',
            help="func")
    parser.add_argument("number", 
            type = int,  
            nargs = "?",
            help="number")
    parser.add_argument("--sleep-time", 
            type = float,  
            default = 1,
            help="sleep time")
    args = parser.parse_args()
    return args

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def produce(args):
    if args.serializer == 'json':
        producer = get_producer_json()
    elif args.serializer == 'avro':
        producer = get_producer_avro(type_ = args.data)
    gen = None
    if args.data == 'words':
        func = make_words
    elif args.data == 'numbers':
        gen = num_gen()
        func = make_numbers

    elif args.data == 'person':
        func = make_person_middle
    if args.verbose:
        print(f'producting to topic {args.topic_name}')
    def wrapper():
        func(producer = producer,
                topic_name = args.topic_name,
                gen = gen,
                key = args.key)
        time.sleep(args.sleep_time)
    if not args.number:
        while True:
            wrapper()
    else:
        for i in range(args.number):
            wrapper()


def make_words(producer, topic_name, key, *args, **kwargs):
    words = ['tiger', 'bear']
    x = datetime.datetime.now().astimezone()
    for j in words:
        data = {'word' : j,
                'timestamp': x}
        producer.send(topic = topic_name, value=data, 
                key = key.encode('utf8'))

def num_gen():
    for i in range(100000):
        yield i

def make_numbers(producer, topic_name, gen, key):
    data = {'number' : next(gen)}
    producer.send(topic_name, 
            value=data,
            key = key.encode('utf8'))

def make_person_middle(producer, topic_name, gen, key):
    data = {'first' : 'Paul',
            'last': 'Tremblay',
            'middle': 'Henry',
            }
    producer.send(topic_name, 
            value=data,
            key = key.encode('utf8'))

def get_producer_json():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x, default = json_serial).encode('utf-8'),
                acks = 'all'
                )
    return producer

def avro_serializer(type_):
    def wrapper(the_dict):
        if type_ == 'words':
            schema_path = 'words.avsc'
        elif type_ == 'numbers':
            schema_path = 'numbers.avsc'
        elif type_ == 'person':
            schema_path = 'person_middle.avsc'
        else:
            raise ValueError('no matching schema')

        schema = avro.schema.parse(open(schema_path).read())
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(the_dict, encoder)
        raw_bytes = bytes_writer.getvalue()
        return raw_bytes
    return wrapper

def get_producer_avro(type_):
    func = avro_serializer(type_)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=func,
                acks = 'all'
                )
    return producer

if __name__ == '__main__':
    args = _get_args()
    produce(args)


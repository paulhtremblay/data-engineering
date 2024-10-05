import os
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
from typing import Generator, Callable

from avro.io import DatumWriter

import kafka
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
            help="type of data to produce")
    parser.add_argument("--number", "-n",  
            type = int,  
            required = False,
            default = None,
            help="number of topics to produces. If no number is passed, producer produces topics forever.")
    parser.add_argument("--sleep-time", 
            type = float,  
            default = 1,
            help="time between each topic")
    args = parser.parse_args()
    return args

def json_serial(obj:object):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def produce(
        topic_name:str,
        serializer:str, 
        key:str,
        data:str, 
        sleep_time:int,
        number: int = None,
        verbose:bool = False):
    """
    produces messages based 
    :param: serializer str: either json, or avro
    :param: topic_name str: any string for topic name
    :param: key str: any string for key
    :param: sleep_time, int: time between topic
    :param: data, str: either words, numbers, or person. Default is words
    :param: number, int: number of messages. If none, messages will be produced indefinitely
    :verbose: whether to print messages

    """
    if data != None:
        assert data in ['words', 'numbers', 'person']
    if serializer == 'json':
        producer = get_producer_json()
    elif serializer == 'avro':
        producer = get_producer_avro(type_ = data)
    gen = None
    if data == 'words':
        func = make_words
    elif data == 'numbers':
        gen = num_gen()
        func = make_numbers

    elif data == 'person':
        func = make_person_middle
    if verbose:
        print(f'producting to topic {topic_name}')
    def wrapper():
        func(producer = producer,
                topic_name = topic_name,
                gen = gen,
                key = key)
        time.sleep(sleep_time)
    if not number:
        while True:
            wrapper()
    else:
        for i in range(number):
            wrapper()


def make_words(
        producer:kafka.producer.kafka.KafkaProducer, 
        topic_name:str, 
        key:str, 
        gen:Generator):
    """
    produces words "tiger" or "bear" with timestamp
    :param: producer, KafkaProducer
    :param: topic_name, str: name of topic
    :param: key, str: not sure as this should be optional
    :gen: generator object
    """
    
    words = ['tiger', 'bear']
    x = datetime.datetime.now().astimezone()
    for j in words:
        data = {'word' : j,
                'timestamp': x}
        producer.send(topic = topic_name, value=data, 
                key = key.encode('utf8'))

def num_gen() -> Generator[int, None, None]:
    """
    generator for numbers
    :returns: generator
    """
    for i in range(100000):
        yield i

def make_numbers(
        producer:kafka.producer.kafka.KafkaProducer, 
        topic_name:str, 
        key:str, 
        gen:Generator):
    """
    produces "number" <number>
    :param: producer, KafkaProducer
    :param: topic_name, str: name of topic
    :param: key, str: not sure as this should be optional
    :gen: generator object
    """
    data = {'number' : next(gen)}
    producer.send(topic_name, 
            value=data,
            key = key.encode('utf8'))

def make_person_middle(
        producer: kafka.producer.kafka.KafkaProducer,
        topic_name:str, 
        gen: Generator, 
        key: str):
    """
    produces 'first': <first> 'last':<last>, 'middle': <middle>
    :param: producer, KafkaProducer
    :param: topic_name, str: name of topic
    :param: key, str: not sure as this should be optional
    :gen: generator object
    """
    data = {'first' : 'Paul',
            'last': 'Tremblay',
            'middle': 'Henry',
            }
    producer.send(topic_name, 
            value=data,
            key = key.encode('utf8'))

def get_producer_json() -> kafka.producer.kafka.KafkaProducer :
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x, default = json_serial).encode('utf-8'),
                acks = 'all'
                )
    return producer

def avro_serializer(type_:str) -> Callable:
    """
    avro serializer. Must be a function for the producer to call
    """
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

def get_producer_avro(type_:str) -> kafka.producer.kafka.KafkaProducer :
    func = avro_serializer(type_)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                value_serializer=func,
                acks = 'all'
                )
    return producer

if __name__ == '__main__':
    args = _get_args()
    produce(serializer = args.serializer,
            data = args.data,
            topic_name = args.topic_name,
            sleep_time = args.sleep_time,
            key = args.key,
            verbose = args.verbose,
            number = args.number,
            )


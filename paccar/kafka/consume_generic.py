from time import sleep
from kafka import KafkaConsumer
from json import dumps
from json import loads
import argparse
import io
from avro.io import BinaryDecoder
from avro.io import DatumReader
import avro.schema
from avro.io import DatumWriter

SCHEMA_PATH_WORDS = "words.avsc"
SCHEMA_WORDS = avro.schema.parse(open(SCHEMA_PATH_WORDS).read())
READER_WORDS = DatumReader(SCHEMA_WORDS)

SCHEMA_PATH_NUMBERS = "numbers.avsc"
SCHEMA_NUMBERS = avro.schema.parse(open(SCHEMA_PATH_NUMBERS).read())
READER_NUMBERS = DatumReader(SCHEMA_NUMBERS)

SCHEMA_PATH_PERSON_MIDDLE = "person_middle.avsc"
SCHEMA_PERSON_MIDDLE = avro.schema.parse(open(SCHEMA_PATH_PERSON_MIDDLE).read())
READER_PERSON_MIDDLE = DatumReader(SCHEMA_PERSON_MIDDLE)

SCHEMA_PATH_PERSON_NO_MIDDLE = "person_no_middle.avsc"
SCHEMA_PERSON_NO_MIDDLE = avro.schema.parse(open(SCHEMA_PATH_PERSON_NO_MIDDLE).read())
READER_PERSON_NO_MIDDLE = DatumReader(SCHEMA_PERSON_NO_MIDDLE)

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("topic", 
            type = str,  
            help="name of topic")
    parser.add_argument("--group-id", '-g', 
            type = str,  
            default = 'my-group',
            help="name of topic")
    parser.add_argument("--deserializer", "-d",
            choices = ['avro','json'],  
            default = 'json',
            help="deserializer")
    parser.add_argument("--schema", 
            choices = ['words','numbers', 
                'person-no-middle', 
                'person-middle'],  
            help="type for deserializer")
    args = parser.parse_args()
    return args

def get_decode(schema, verbose):
    if verbose:
        print(f'schema type is {schema}')
    def wrapper(msg_value):
        if schema == 'words':
            reader = READER_WORDS
        elif schema == 'numbers':
            reader = READER_NUMBERS
        elif schema == 'person-no-middle':
            reader = READER_PERSON_NO_MIDDLE
        elif schema == 'person-middle':
            reader = READER_PERSON_MIDDLE
        else:
            raise ValueError('no match')
        message_bytes = io.BytesIO(msg_value)
        message_bytes.seek(0)
        decoder = BinaryDecoder(message_bytes)
        event_dict = reader.read(decoder)
        return event_dict
    if verbose:
        print(f'wrapper is {wrapper}')
    return wrapper

def get_consumer(topic_name, group_id, 
        deserializer,
        schema,
        enable_auto_commit = True,
        verbose = False):
    if deserializer == 'json':
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    else:
        decode = get_decode(schema = schema, verbose = verbose)
        value_deserializer = decode
    if verbose and deserializer == 'avro':
        print(f'decode is {decode}')
    consumer = KafkaConsumer(
        topic_name,
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=enable_auto_commit,
         group_id=group_id,
         value_deserializer=value_deserializer
         )
    return consumer

def get_data(consumer):
    while True:
        poll_messages = consumer.poll()

        for message in consumer:
            print(message.value)

def get_data_(consumer):
    for message in consumer:
        message = message.value
        print(message)

def main(topic_name, group_id, 
        deserializer, schema = None, 
        verbose = False):
    consumer = get_consumer(
            topic_name = topic_name,
            deserializer = deserializer,
            group_id = group_id,
            schema = schema,
            verbose = verbose
            )
    get_data(consumer = consumer)

if __name__ == '__main__':
    args = _get_args()
    main(topic_name = args.topic,
            deserializer = args.deserializer,
            group_id = args.group_id,
            schema = args.schema,
            verbose = args.verbose
            )


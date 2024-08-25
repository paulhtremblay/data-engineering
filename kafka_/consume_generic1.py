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

schema_path = "words.avsc"
schema = avro.schema.parse(open(schema_path).read())
READER = DatumReader(schema)

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
    args = parser.parse_args()
    return args

def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(0)
    decoder = BinaryDecoder(message_bytes)
    event_dict = READER.read(decoder)
    return event_dict

def get_consumer(topic_name, group_id, 
        deserializer,
        enable_auto_commit = True):
    if deserializer == 'json':
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    else:
        value_deserializer = decode
    consumer = KafkaConsumer(
        topic_name,
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=enable_auto_commit,
         group_id=group_id,
         value_deserializer=value_deserializer)
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
        deserializer, verbose = False):
    consumer = get_consumer(
            topic_name = topic_name,
            deserializer = deserializer,
            group_id = group_id)
    get_data(consumer = consumer)

if __name__ == '__main__':
    args = _get_args()
    main(topic_name = args.topic,
            deserializer = args.deserializer,
            group_id = args.group_id)


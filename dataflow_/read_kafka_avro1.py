import logging
import sys
import typing
import json

import avro.schema
from avro.io import DatumReader
import io

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

import argparse

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--temp_location', '-tl',
             required = True,
             help='bucket ')
    parser.add_argument( '--topic', 
             required = True,
             help='topic ')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DataflowRunner',
        help='runner')
    parser.add_argument( '--template_location', '-t',
            default = None,
        help='if creating a template')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def convert_kafka_record_to_dictionary(record):
    key = record[0]
    data = record[1]
    schema_path = '../kafka_/words.avsc'
    schema = avro.schema.parse(open(schema_path).read())
    reader = DatumReader(schema)
    bytes_reader = io.BytesIO(data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    deserialized_json = reader.read(decoder)
    return deserialized_json

def run(
    bootstrap_servers,
    ):
    jsonFormatSchema = open("../kafka_/words.avsc", "r").read()
    known_args, pipeline_args = _get_args()
    pipeline_options = PipelineOptions(
        pipeline_args, 
        streaming=False, 
        save_main_session=True,
        template_location= known_args.template_location,
    )
    project = 'paul-henry-tremblay'
    pipeline_args = ['--region',  'us-central1',  '--project',project , '--temp_location',  
          f'gs://{known_args.temp_location}', '--runner', known_args.runner] 
    with beam.Pipeline(options=pipeline_options) as pipeline:

        ride_col = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': bootstrap_servers},
                topics=[known_args.topic],
                max_num_records = 2,
                with_metadata=False)
            | beam.Map(lambda record: convert_kafka_record_to_dictionary(record))
            | beam.Map(print)
            )

if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run(
            bootstrap_servers = "localhost:9092",
            )

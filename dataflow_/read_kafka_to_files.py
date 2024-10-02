import logging
import sys
import typing
import json
import datetime
import time

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import WriteToFiles
"""
"""

import argparse

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--topic', 
             required = True,
             help='topic ')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DirectRunner',
        help='runner')
    parser.add_argument( '--project', 
            default = 'paul-henry-tremblay',
        help='project')
    parser.add_argument( '--template_location', '-t',
            default = None,
        help='if creating a template')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def convert_kafka_record_to_dictionary(record):
    key = record[0]
    data = record[1]
    d = record.value
    record.offset
    data = json.loads(d)
    return  data

def dump_to_json(element):
    return json.dumps(element)

def run(
    bootstrap_servers,
    ):
    known_args, pipeline_args = _get_args()
    pipeline_options = PipelineOptions(
        pipeline_args, 
        streaming=False, 
        save_main_session=True,
        template_location= known_args.template_location,
    )
    pipeline_options = PipelineOptions(
        region= 'us-central1',
        project= known_args.project, 
        runner= known_args.runner,
        streaming=True, 
    )
    with beam.Pipeline(options=pipeline_options) as pipeline:

        ride_col = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': bootstrap_servers,
                    'group.id': 'my-group',
                    'isolation.level': 'read_uncommitted',
                    },
                topics=[known_args.topic],
                max_num_records = 2,
                commit_offset_in_finalize = True,
                start_read_time = int(time.mktime(datetime.datetime(2024,10,1).timetuple())),
                with_metadata=True)
            | beam.Map(lambda record: convert_kafka_record_to_dictionary(record))
            | "dump to json" >> beam.Map(dump_to_json)
            | WriteToFiles("output/create1_simple")
            )

if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run(
            bootstrap_servers = "localhost:9092",
            )

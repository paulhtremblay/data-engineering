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
from apache_beam.options.pipeline_options import SetupOptions
"""
"""

import argparse

class RunTimeOptions(PipelineOptions):
    """
    used when running 
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')
        parser.add_value_provider_argument(
          '--out',
          type = str,
          required = True,
          help='bucket/folder for output')


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DirectRunner',
        help='runner')
    parser.add_argument( '--project',
             required = False,
             default = 'paul-henry-tremblay',
             help='bucket ')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def convert_kafka_record_to_dictionary(record):
    key = record[0]
    data = record[1]
    d = record.value
    record.offset
    data = json.loads(d)
    return  data

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def dump_to_json(element):
    return json.dumps(element, default = json_serial)

def _get_topics():
    with open('dynamic_topics.txt', 'r') as read_obj:
        lines = read_obj.readlines()[0].strip()
    return [lines]


def run(
    bootstrap_servers,
    ):

    known_args, pipeline_args = _get_args()

    pipeline_options = PipelineOptions(
        region= 'us-central1',
        project= known_args.project, 
        runner= known_args.runner,
        streaming=True, 
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_options = pipeline_options.view_as(RunTimeOptions)
    topics = _get_topics()

    with beam.Pipeline(options=pipeline_options) as pipeline:

        ride_col = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': bootstrap_servers,
                    'group.id': 'my-group',
                    'isolation.level': 'read_uncommitted',
                    },
                topics=topics,
                max_num_records = 2, 
                commit_offset_in_finalize = True,
                with_metadata=True)
            | beam.Map(lambda record: convert_kafka_record_to_dictionary(record))
            | "dump to json" >> beam.Map(dump_to_json)
        | WriteToFiles(runtime_options.out)
            )

if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run(
            bootstrap_servers = "localhost:9092",
            )

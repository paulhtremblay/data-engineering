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
from apache_beam.transforms.periodicsequence import PeriodicImpulse
"""
"""

import argparse

class RunTimeOptions(PipelineOptions):
    """
    used when running the batch job
    (Cannot be used streaming?)
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--out',
          type = str,
          required = True,
          help='bucket/folder for output')
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--topic', 
             required = True,
             help='topic ')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner', 'FlinkRunner'],
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

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def dump_to_json(element):
    return json.dumps(element, default = json_serial)

def create_side_data(element):
        return  {'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

def  merge_with_side(element, side):
    d = json.loads(element[1][0])
    d['time'] = side['time']
    return d

def run(
    bootstrap_servers,
    ):
    known_args, pipeline_args = _get_args()
    if known_args.runner == 'FlinkRunner':
        pipeline_options = PipelineOptions(
            region= 'us-central1',
            project= known_args.project, 
            runner= 'FlinkRunner',
            flink_master="localhost:8081",
            environment_type="LOOPBACK",
            streaming=True, 
        )
    elif known_args.runner in ['DirectRunner', 'DataflowRunner']:
        pipeline_options = PipelineOptions(
            region= 'us-central1',
            project= known_args.project, 
            runner= known_args.runner,
            streaming=True, 
            )
    else:
        raise ValueError(f'no runner for {known_args.runner}')
    runtime_options = pipeline_options.view_as(RunTimeOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:

        main = (
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
            )
        side_input = (
            pipeline
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=datetime.datetime.now().timestamp(),
                fire_interval=10,
                apply_windowing=True,
            )
            | 'create data' >> beam.Map(create_side_data)
            )
        main | ( 
             "merge" >> beam.Map(merge_with_side, side = beam.pvalue.AsSingleton(side_input))
            | "to json" >> beam.Map(dump_to_json)
            | "output to text" >>  WriteToFiles(runtime_options.out)
                
                )

if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run(
            bootstrap_servers = "localhost:9092",
            )

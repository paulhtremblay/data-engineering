import apache_beam as beam
from apache_beam import DoFn, ParDo
import argparse
import os
import sys
import datetime
import warnings
import logging

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

DATA = [
        {'serverID': 'server_1', 'CPU_Utilization': 0, 'timestamp': 1},
        {'serverID': 'server_2', 'CPU_Utilization': 10, 'timestamp': 1},
        {'serverID': 'server_3', 'CPU_Utilization': 20, 'timestamp': 3},
        {'serverID': 'server_1', 'CPU_Utilization': 30, 'timestamp': 2},
        {'serverID': 'server_2', 'CPU_Utilization': 40, 'timestamp': 3},
        {'serverID': 'server_3', 'CPU_Utilization': 50, 'timestamp': 6},
        {'serverID': 'server_1', 'CPU_Utilization': 60, 'timestamp': 7},
        {'serverID': 'server_2', 'CPU_Utilization': 70, 'timestamp': 7},
        {'serverID': 'server_3', 'CPU_Utilization': 80, 'timestamp': 14},
        {'serverID': 'server_2', 'CPU_Utilization': 85, 'timestamp': 8.5},
        {'serverID': 'server_1', 'CPU_Utilization': 90, 'timestamp': 14}
    ]

def _get_date_time(s):
    return s

class RunTimeOptions(PipelineOptions):
    """
    used when running the batch job
    (Cannot be used streaming?)
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--run_num',
          type = int,
          required = True,
          help='timestamp')
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')


def _get_args():
    """
    passed when creating the template
    """
    parser = argparse.ArgumentParser()
    parser.add_argument( '--project', '-p',
             choices = ['paul-henry-tremblay'],
             required = True,
        help='project')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DirectRunner',
        help='runner')
    parser.add_argument( '--template_location', '-t',
            default = None,
        help='if creating a template')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

class AddTimestamp(DoFn):

    def __init__(self, run_num):
        self.run_num = run_num

    def process(self, element, *args, **kwargs):
        element['run_num'] = self.run_num.get()
        yield element



def run():
    known_args, pipeline_args = _get_args()
    project = known_args.project
    pipeline_options = PipelineOptions(
        region= 'us-central1',
        project= project, 
        runner= known_args.runner,
        streaming=False, 
    )
    runtime_options = pipeline_options.view_as(RunTimeOptions)
    print(f'runtime_options {runtime_options}')
    
    pipeline_options.view_as(SetupOptions).save_main_session = True
    verbose = runtime_options.verbose == 'true'

    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | 'Create Events' >> beam.Create(DATA) \
        | 'add timestamp' >> ParDo(AddTimestamp(run_num =runtime_options.run_num)) \
        | 'print1' >> beam.Map(print)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()

import os
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
#from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions


from  dependencies import lib1 

DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def _get_args():
    """
    passed when creating the template
    """
    parser = argparse.ArgumentParser()
    parser.add_argument( '--project', '-p',
             choices = ['paul-henry-tremblay'],
            default = 'paul-henry-tremblay',
             required = False,
        help='project')
    parser.add_argument( '--region', 
            default = 'us-west1' ,
        help='region')
    parser.add_argument( '--verbose', 
            default = 0 ,
            required = False,
        help='verbose level ')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DirectRunner',
        help='runner')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


class RunTimeOptions(PipelineOptions):
    """
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--verbosity',
          type = int,
          default = 0,
          required=False,
          help='verbose.')

class Something1(beam.DoFn):

    def __init__(self, verbosity):
        self.verbosity = verbosity

    def process(self, element):
        logging.debug('In debug process')
        logging.info('In process')
        logging.error('In process')
        yield element

class Something2(beam.DoFn):

    def __init__(self, verbosity):
        self.verbosity = verbosity

    def process(self, element):
        yield lib1.func1(element, verbosity = self.verbosity)

def run(save_main_session=True):
    known_args, pipeline_args = _get_args()
    pipeline_options = PipelineOptions(
        region= known_args.region,
        project= known_args.project, 
        runner= known_args.runner,
        streaming=False, 
    )

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    worker_options = pipeline_options.view_as(WorkerOptions)
    runtime_options = pipeline_options.view_as(RunTimeOptions)
    try:
        verbosity = runtime_options.verbosity.get()
    except beam.error.RuntimeValueProviderError:
        verbosity = known_args.verbose
    if verbosity >=3:
        worker_options.default_sdk_harness_log_level = 'DEBUG'
        logging.getLogger().setLevel(logging.DEBUG)
    elif verbosity >=2:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.ERROR)
    logging.debug('before pipeline')
    logging.info('before pipeline')
    logging.error('before pipeline')




    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | 'something1' >> (beam.ParDo(Something1(verbosity = verbosity)))
            | 'something2' >> (beam.ParDo(Something2(verbosity = verbosity)))
            | 'debug' >> beam.Map(print)
        )

if __name__ == '__main__':
  run()

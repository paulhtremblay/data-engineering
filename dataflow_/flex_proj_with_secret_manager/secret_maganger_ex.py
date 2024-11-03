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

try:
    import tothe
except ModuleNotFoundError:
    pass

DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def _get_args():
    """
    passed when creating the template
    """
    parser = argparse.ArgumentParser()
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DirectRunner',
        help='runner')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


class Transform(beam.DoFn):

    def process(self, element):
        lib1.get_secret(project_id = 'paul-henry-tremblay',
                secret_id = 'test1')
        yield element


def run(save_main_session=True):
    known_args, pipeline_args = _get_args()
    pipeline_options = PipelineOptions(
        region= 'us-west1',
        project= 'paul-henry-trembaly', 
        runner= known_args.runner,
        streaming=False, 
    )
    lib1.get_secret(project_id = 'paul-henry-tremblay',
                secret_id = 'test1')

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | 'transform' >> (beam.ParDo(Transform()))
            | 'debug' >> beam.Map(print)
        )

if __name__ == '__main__':
  run()

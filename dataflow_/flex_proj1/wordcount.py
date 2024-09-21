"""A word-counting workflow."""

import os
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from dependencies.lib1 import TEST_VAR2

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
    parser.add_argument( '--region', 
            default = 'us-west1' ,
        help='region')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


class RunTimeOptions(PipelineOptions):
    """
    used when running the batch job
    (Cannot be used streaming?)
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--output',
          required = True,
          help='output')
        parser.add_value_provider_argument(
          '--input',
          required = True,
          help='input')
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')

class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    #os.environ['TEST_VAR']
    TEST_VAR2
    return re.findall(r'[\w\']+', element, re.UNICODE)

def format_result(word, count):
  return '%s: %d' % (word, count)

def run(save_main_session=True):
    known_args, pipeline_args = _get_args()
    #image_url = 'us-west1-docker.pkg.dev/paul-henry-tremblay/dataflow-ex/dataflow/d-image:tag1'
    pipeline_options = PipelineOptions(
        region= known_args.region,
        project= known_args.project, 
        runner= known_args.runner,
        streaming=False, 
    )

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    runtime_options = pipeline_options.view_as(RunTimeOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(runtime_options.input)
        counts = (
            lines
            | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.MapTuple(format_result)
            | 'Write' >> WriteToText(runtime_options.output)
            )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

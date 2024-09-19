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


class RunTimeOptions(PipelineOptions):
    """
    used when running the batch job
    (Cannot be used streaming?)
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--output',
          required = False,
          help='output')
        parser.add_value_provider_argument(
          '--input',
          required = False,
          help='input')
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')

class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    os.environ['TEST_VAR']
    TEST_VAR2
    return re.findall(r'[\w\']+', element, re.UNICODE)

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  """
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=False,
      help='Output file to write results to.')
  """
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  runtime_options = pipeline_options.view_as(RunTimeOptions)

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(runtime_options.input)

    counts = (
        lines
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    def format_result(word, count):
      return '%s: %d' % (word, count)
    output = counts | 'Format' >> beam.MapTuple(format_result)
    output | 'Write' >> WriteToText(runtime_options.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

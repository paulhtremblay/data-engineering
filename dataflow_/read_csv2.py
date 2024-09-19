import logging
import sys
import typing
import json
import datetime
import time
import csv

import apache_beam as beam
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
import apache_beam.io.textio 
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
"""
"""
print(dir(apache_beam.io.textio ))
assert False


import argparse

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--temp_location', '-tl',
             required = True,
             help='bucket ')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DirectRunner',
        help='runner')
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

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line

def run(
    ):
    known_args, pipeline_args = _get_args()
    pipeline_options = PipelineOptions(
        pipeline_args, 
        streaming=True, 
        save_main_session=True,
        template_location= known_args.template_location,
    )
    project = 'paul-henry-tremblay'
    pipeline_args = ['--region',  'us-central1',  '--project',project , '--temp_location',  
          f'gs://{known_args.temp_location}', '--runner', known_args.runner] 
    with beam.Pipeline(options=pipeline_options) as pipeline:
        r = (pipeline.apply(TextIO.read()))
    """
    with beam.Pipeline(options=pipeline_options) as pipeline:
        x = (pipeline | 'Read input file' >> beam.io.ReadFromText('temp/*')
                | 'Parse file' >> beam.Map(parse_file)
                | 'Print output' >> beam.Map(print)
                )
    """

if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run(
            )

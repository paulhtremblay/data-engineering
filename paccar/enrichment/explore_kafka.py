import argparse
from datetime import datetime, date
import logging
import json

import yaml

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline

from dependencies import get_configs
from apache_beam import PTransform


"""
to test

in another shell run:
python publish_pub_sub.py test

"""

class RunTimeOptions(PipelineOptions):
    """
    used when running the batch job
    (Cannot be used streaming?)
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--topic',
             required = True,
             help='topic ')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def dump_to_json(element):
    return json.dumps(element, default = json_serial)

def get_value(element):
    for i in dir(element):
        if i.startswith('_'):
            continue
        print(i)
    topic = element.topic
    value = element.value
    print(f'topic is {topic} and value is {value}')
    return element


def run():
    known_args, pipeline_args = _get_args()
    pipeline_options = PipelineOptions(
        region= 'us-westl',
        runner= 'DirectRunner',
        streaming=True, 
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_options = pipeline_options.view_as(RunTimeOptions)

    max_num_records  = 2

    with Pipeline(options=pipeline_options) as pipeline:
        main = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': 'localhost:9092',
                    'group.id': 'my-group',
                    'isolation.level': 'read_uncommitted',
                    },
                topics= [known_args.topic],
                max_num_records = max_num_records, 
                commit_offset_in_finalize = True,
                with_metadata=True)
            | beam.Map(get_value)
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()

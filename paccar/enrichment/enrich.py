import argparse
from datetime import datetime, date
import logging
import random
import json
import typing

import yaml

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline

from dependencies import get_configs
from apache_beam import PTransform
from apache_beam import WindowInto
from apache_beam.transforms.window import FixedWindows
from apache_beam import  WithKeys
from apache_beam import GroupByKey

from apache_beam.coders import Coder, BytesCoder


#from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
#from apache_beam.options.pipeline_options import PipelineOptions
#from apache_beam.options.pipeline_options import SetupOptions
#from apache_beam.transforms.window import FixedWindows
#import apache_beam as beam
#from apache_beam.transforms.periodicsequence import PeriodicImpulse
#from apache_beam.io.fileio import WriteToFiles
"""
to test

in another shell run:
python publish_pub_sub.py ingest_test

"""

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
          help='out topic')
        parser.add_value_provider_argument(
          '--in',
          type = str,
          required = True,
          help='in topic')
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--project',
             required = False,
             default = 'paul-henry-tremblay',
             help='bucket ')
    parser.add_argument( '--runner',
             choices = ['DataflowRunner', 'DirectRunner', 'FlinkRunner'],
             default = 'DirectRunner',
             help='runner to use ')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args



class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        self.window_size = int(window_size )
        self.num_shards = num_shards

    def expand(self, pcoll):
        """
        returns a tuple

        """
        return (
            pcoll
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            #| "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            #random key added 
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            | "Group by key" >> GroupByKey()
        )


def  merge_with_side(element, side):
    d = json.loads(element[1][0])
    d['time'] = side['time']
    return d


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def dump_to_json(element):
    return json.dumps(element, default = json_serial)

def create_side_data(element):
        return  {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

def _get_topics():
    with open('dynamic_topics.txt', 'r') as read_obj:
        lines = read_obj.readlines()[0].strip()
    return [lines]

def get_value(element):
    #because we sharded and grouped by
    print(element)
    shard_num = element[0]
    kafka_obj = element[1][0] # all the topics within the window
    value = kafka_obj.value
    return ('a'.encode('utf8'), value)


def run(window_size=10.0, num_shards=5):
    known_args, pipeline_args = _get_args()
    window_size = 1.0
    if known_args.runner == 'FlinkRunner':
        pipeline_options = PipelineOptions(
            region= 'us-westl',
            project= known_args.project, 
            runner= known_args.runner,
            flink_master="localhost:8081",
            environment_type="LOOPBACK",
            streaming=True, 
        )
    elif known_args.runner in ['DirectRunner', 'DataflowRunner']:
        pipeline_options = PipelineOptions(
            region= 'us-westl',
            project= known_args.project, 
            runner= known_args.runner,
            streaming=True, 
        )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_options = pipeline_options.view_as(RunTimeOptions)
    topics = _get_topics()
    configs = get_configs.get_configs()
    print(configs)

    if known_args.runner == 'DirectRunner':
        max_num_records = int(configs['max_num_records'])
    else:
        max_num_records = None 

    with Pipeline(options=pipeline_options) as pipeline:
        main = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': configs['bootstrap_servers'],
                    'group.id': 'my-group',
                    'isolation.level': 'read_uncommitted',
                    },
                topics= configs['topics'],
                max_num_records = max_num_records, 
                commit_offset_in_finalize = True,
                with_metadata=True)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards) \
            | beam.Map(get_value).with_output_types(typing.Tuple[bytes, bytes])
            | "Write to Kafka" >>  WriteToKafka(
                producer_config={'bootstrap.servers': configs['bootstrap_servers']},
                    topic=configs['out_topic'],
                    #key_serializer=MySerializer,
                    #value_serializer=MySerializer,

                    )
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()

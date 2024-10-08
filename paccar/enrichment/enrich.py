import argparse
from datetime import datetime, date
import logging
import random
import json
import typing

from typing import Tuple, List, Any, Iterable

import yaml

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline
from apache_beam import PTransform
from apache_beam import WindowInto
from apache_beam.transforms.window import FixedWindows
from apache_beam import  WithKeys
from apache_beam import GroupByKey
import apache_beam.typehints.schemas 
from apache_beam.typehints.schemas  import Any as BeamAny


from enrichment_dependencies import get_configs
from enrichment_dependencies import get_topics

"""
to test

in another shell run:
python publish_pub_sub.py ingest_test

"""

class RunTimeOptions(PipelineOptions):
    """
    Get options for run time (that can be passed at runtime)
    """

    @classmethod #create a property from a method
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--out',
          type = str,
          required = True,
          help='out topic; note, this argument is ignored for now, as out is determined by a config file')

def _get_args() -> (argparse.Namespace, list):
    parser = argparse.ArgumentParser()
    parser.add_argument( '--project',
             required = False,
             default = 'paccar-trusted-data-nonprod',
             help='project ')
    parser.add_argument( '--runner',
             choices = ['DataflowRunner', 'DirectRunner', 'FlinkRunner'],
             default = 'DirectRunner',
             help='runner to use ')
    parser.add_argument(
          '--verbose',
          type = int,
          required=False,
          default = 0,
          help='verbose level, with 0 being not messages at all.')

    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Kafka messages based on publish time
    and outputs a list of tuples, each containing a message.
    """

    def __init__(self, window_size:float, num_shards:int=5):
        self.window_size = int(window_size )
        self.num_shards = num_shards

    def expand(self, pcoll:apache_beam.pvalue.PCollection) -> apache_beam.pvalue.PCollection :
        
        """
        Group messages into shards, with each shard containing a list of topics
        
        :param: pcol, apache beam pcollection

        """
        return (
            pcoll
            | "Window into fixed intervals" >> WindowInto(FixedWindows(self.window_size))
            #random key added 
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            | "Group by key" >> GroupByKey()
        )



def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def _dump_to_json(element:object):
    """
    for debugging purposes
    """
    return json.dumps(element, default = json_serial)

def create_side_data(element):
        return  {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

def get_value(element:Tuple[Any, Iterable[BeamAny]]) :
    print(f'type of element is {type(element)}')
    print(f'type of element[0] is {type(element[0])}')
    print(f'type of element[1] is {type(element[1])}')
    print(f'type of element[1][0] is {type(element[1][0])}')
    #because we sharded and grouped by
    print(element)
    shard_num = element[0]
    kafka_obj = element[1][0] # all the topics within the window
    value = kafka_obj.value
    return ('a'.encode('utf8'), value)

def prepare_for_kafka_write(element, key):
    return (key.encode('utf8'), json.dumps(element).encode('utf8'))

def _make_pipeline_options(known_args:argparse.Namespace) -> apache_beam.options.pipeline_options.PipelineOptions:
    """
    create pipeline_options needed for pipeline
    TODO: this probabably could be abstracted and re-used

    :param: known_args, ??: arguments from command line (but not run time)
    :returns: apache_beam.options.pipeline_options.PipelineOptions: a class for runtime options
    """
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
    return pipeline_options


def run(window_size:float=10.0, num_shards:int=5):
    """
    main entry point to run Apache Beam Job

    :param: window_size float: the number of seconds for the window 
            TODO This probabaly should not be hard coded
    :param: num_shards int: the number of shards to break the topics into. 
            This is done do a large number of messages can be parallelized
    """
    known_args, pipeline_args = _get_args()
    pipeline_options = _make_pipeline_options(known_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_options = pipeline_options.view_as(RunTimeOptions)
    if known_args.verbose > 0:
        print(configs)
    configs = get_configs.get_configs()
    topics = get_topics.get_topics(runner = known_args.runner)
    if known_args.verbose > 0:
        print(f'topics are {topics}')

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
                topics= topics,
                max_num_records = max_num_records, 
                commit_offset_in_finalize = True,
                with_metadata=True)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards) \
            | beam.Map(get_value).with_output_types(typing.Tuple[bytes, bytes])
            | "Write to Kafka" >>  WriteToKafka(
                producer_config={'bootstrap.servers': configs['bootstrap_servers']},
                    topic=configs['out_topic'],

                    )
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()

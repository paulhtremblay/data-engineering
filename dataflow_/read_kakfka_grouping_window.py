import argparse
from datetime import datetime, date
import logging
import random
import json
import typing

from typing import Tuple, List, Any, Iterable


import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
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



"""
to test

in another shell run:
python publish_pub_sub.py ingest_test

"""


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



def get_value(element) :
    shard_num = element[0]
    print(f'shard_num is {shard_num} and len is {len(element[1])}')

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
    max_num_records = 1000
    with Pipeline() as pipeline:
        main = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': "localhost:9092",
                    'group.id': 'my-group',
                    'isolation.level': 'read_uncommitted',
                    },
                topics= ['test'],
                max_num_records = max_num_records, 
                commit_offset_in_finalize = True,
                with_metadata=True)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards) \
            | beam.Map(get_value)
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run(window_size = 60)


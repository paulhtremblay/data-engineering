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
import apache_beam.typehints.schemas 
from apache_beam.typehints.schemas  import Any as BeamAny


from apache_beam.io.restriction_trackers import OffsetRange

"""
to test

in another shell run:
python publish_pub_sub.py ingest_test

"""


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

    def __init__(self, window_size:float):
        self.window_size = int(window_size )

    def expand(self, pcoll:apache_beam.pvalue.PCollection) -> apache_beam.pvalue.PCollection :
        
        """
        Group messages into shards, with each shard containing a list of topics
        
        :param: pcol, apache beam pcollection

        """
        return (
            pcoll
            | "Window into fixed intervals" >> WindowInto(FixedWindows(self.window_size))
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


class FileToWordsRestrictionProvider(beam.transforms.core.RestrictionProvider
                                     ):

    def initial_restriction(self, list_):
        return OffsetRange(0, len(list_))

    def create_tracker(self, restriction):
        return beam.io.restriction_trackers.OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.size()

class SplitFn(beam.DoFn):

    def __init__(self, split_size = 1):
        self.split_size = split_size

    def process(
        self,
        list_,
        tracker=beam.DoFn.RestrictionParam(FileToWordsRestrictionProvider())):
        counter = 0
        while tracker.try_claim(counter):
            yield get_value(list_[counter: counter + self.split_size])
            counter += self.split_size

def get_value(list_):
    l = []
    for i in list_:
        l.append(i.value)
    return l

def just_list(element):
    return [element]


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

    """
    known_args, pipeline_args = _get_args()
    pipeline_options = _make_pipeline_options(known_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    max_num_records = 10
    with Pipeline(options=pipeline_options) as pipeline:
        main = (
            pipeline
            | ReadFromKafka(
                consumer_config={'bootstrap.servers': 'localhost:9092',
                    'group.id': 'my-group',
                    'isolation.level': 'read_uncommitted',
                    },
                topics= ['ingest_test'],
                max_num_records = max_num_records, 
                commit_offset_in_finalize = True,
                with_metadata=True)
            | "Window into" >> GroupMessagesByFixedWindows(window_size) 
            | 'just list' >> beam.Map(just_list)
            |'split' >> beam.ParDo(SplitFn())
            | beam.Map(print)

                    
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()

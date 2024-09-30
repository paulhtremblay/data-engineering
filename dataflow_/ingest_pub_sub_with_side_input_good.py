import argparse
from datetime import datetime, date
import logging
import random
import json

from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.window import FixedWindows
import apache_beam as beam
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io.fileio import WriteToFiles
"""
to test

in another shell run:
python publish_pub_sub.py testtopic

"""

class RunTimeOptions(PipelineOptions):
    """
    used when running the batch job
    (Cannot be used streaming?)
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
          '--out_bucket',
          type = str,
          required = True,
          help='bucket/folder for output')
        parser.add_value_provider_argument(
          '--verbose',
          required=False,
          help='verbose.')


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( '--temp_location', '-tl',
             required = True,
             help='bucket ')
    parser.add_argument( '--project',
             required = False,
             default = 'paul-henry-tremblay',
             help='bucket ')
    parser.add_argument(
          '--subscription',
          type = str,
          required = False,
          default = 'testtopic-sub',
          help='subscription')
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

def run(window_size=10.0, num_shards=5):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    known_args, pipeline_args = _get_args()
    subscription = f'projects/{known_args.project}/subscriptions/{known_args.subscription}'
    window_size = 1.0
    pipeline_options = PipelineOptions(
        region= 'us-central1',
        project= known_args.project, 
        runner= 'DataflowRunner',
        streaming=True, 
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_options = pipeline_options.view_as(RunTimeOptions)

    with Pipeline(options=pipeline_options) as pipeline:
        main = (pipeline | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription = subscription) 
        | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards) 
        )
        side_input = (
            pipeline
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=datetime.now().timestamp(),
                fire_interval=10,
                apply_windowing=True,
            )
            | 'create data' >> beam.Map(create_side_data)
            )
        
        main | ( 
             "merge" >> beam.Map(merge_with_side, side = beam.pvalue.AsSingleton(side_input))
            | "to json" >> beam.Map(dump_to_json)
            | "output to text" >>  WriteToFiles(runtime_options.out_bucket)
                
                )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

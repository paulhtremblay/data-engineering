import argparse
from datetime import datetime
import logging
import random
import json

from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import apache_beam as beam
from apache_beam.transforms.periodicsequence import PeriodicImpulse
"""
to test

in another shell run:
python publish_pub_sub.py testtopic

"""

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


class ProcessPubSubDoFn(beam.DoFn):
  """parse pub/sub message."""

  def __init__(self, side):
      self.side = side

  def process(self, element):
      for i in element[1]:
          d = json.loads(i)
          for key in d.keys():
              yield (d[key], self.side)
          
          #yield (d['value'], side)
    

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


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )

class CreateData(beam.DoFn):
    """
    example
    """

    def process(self, element):
        yield {'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}


def run(window_size=10.0, num_shards=5, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    known_args, pipeline_args = _get_args()
    project = 'paul-henry-tremblay'
    subscription = 'projects/{project}/subscriptions/testtopic-sub'.format(project = project)
    window_size = 1.0
    bucket = 'none'
    pipeline_args = ['--region',  'us-central1',  '--project',project , '--temp_location',  
          f'gs://{known_args.temp_location}', '--runner', known_args.runner] 
    pipeline_options = PipelineOptions(
        pipeline_args, 
        streaming=True, 
        save_main_session=True,
        template_location= known_args.template_location,
        runner = 'FlinkRunner', 
        flink_master="localhost:8081",
        environment_type="LOOPBACK"
    )
    side = {'time':datetime(2024,9,29, 13)}

    with Pipeline(options=pipeline_options) as pipeline:
        main = pipeline | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription = subscription) \
        | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards) \
        | "process pub/sub" >> ParDo(ProcessPubSubDoFn(side = side)) 
        side_input = (
            pipeline
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=datetime.now().timestamp(),
                fire_interval=10,
                apply_windowing=True,
            )
            | 'create data' >> beam.ParDo(CreateData())
            )
        
        main | ("print2" >> beam.Map(print)
                )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    run(
    )

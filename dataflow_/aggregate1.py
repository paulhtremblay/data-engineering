import argparse
from datetime import datetime
from datetime import timezone
import logging
import random
import json
import pprint
pp = pprint.PrettyPrinter(indent = 4)

"""
For a session window, the gap bewteen each determines the window size
"""

from apache_beam import Pipeline, GroupBy, PTransform, ParDo
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows, Sessions
import apache_beam as beam
DATA = [
        {'user': 'user_1', 'bytes_used': 1, 'timestamp': 
            datetime(2024,1,1,0,0,0, tzinfo = timezone.utc)},
        {'user': 'user_2', 'bytes_used': 10, 'timestamp': 
            datetime(2024,1,1,0,0,0, tzinfo = timezone.utc)},
        {'user': 'user_1', 'bytes_used': 2, 'timestamp': 
            datetime(2024,1,1,0,0,1, tzinfo = timezone.utc)},
        {'user': 'user_2', 'bytes_used': 20, 'timestamp': 
            datetime(2024,1,1,0,0,59, tzinfo = timezone.utc)},
    ]

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

class TimeDiff(beam.DoFn):
    """
    calculate the time diff
    """
    def process(self, element):
        d = {'key': element.key,
            'start_time': element.start_time,
            'end_time': element.end_time,
            'total': element.total
                }
        d['time_diff'] = (d['end_time'] - d['start_time']).total_seconds()
        yield d

    def process_(self, element):
        x = element._asdict()
        x['time_diff'] = (x['end_time'] - x['start_time']).total_seconds()
        yield x


class ProcessPubSubDoFn(beam.DoFn):
  """parse pub/sub message."""
  def process(self, element):
      return element
    

class GroupMessagesBySessions(PTransform):
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
            >> WindowInto(Sessions(5))
        )


class AddKeyInfoFn(beam.DoFn):
    """output tuple of window(key) + element(value)"""
    def process(self, element, window=beam.DoFn.WindowParam):
        yield (element['user'], element)

class ToDict(beam.DoFn):

    def process(self, element):
        yield json.loads(element)

class PrintElements(beam.DoFn):
    def process(self, element):
        pp.pprint(element)
        print('\n\n')

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
        streaming=False, 
        save_main_session=True,
        template_location= known_args.template_location,
    )

    with Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline | 'Create Events' >> beam.Create(DATA) 
        | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) 
        | 'group by' >> GroupBy(lambda x: x['user']) \
            .aggregate_field(lambda x: x['bytes_used'], sum, 'total') 
            .aggregate_field(lambda x: x['timestamp'], min, 'start_time') 
            .aggregate_field(lambda x: x['timestamp'], max, 'end_time') 
        | 'get time diff' >> beam.ParDo(TimeDiff())
        | 'print1' >> beam.ParDo(PrintElements())
        )



if __name__ == "__main__":
    #logging.getLogger().setLevel(logging.INFO)

    run(
    )
import argparse
from datetime import datetime
import logging
import random
import json
import pprint
pp = pprint.PrettyPrinter(indent = 4)

"""
For a session window, the gap bewteen each determines the window size
"""

from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows, Sessions
import apache_beam as beam
DATA = [
        {'serverID': 'server_1', 'CPU_Utilization': 0, 'timestamp': 1},
        {'serverID': 'server_2', 'CPU_Utilization': 10, 'timestamp': 1},
        {'serverID': 'server_3', 'CPU_Utilization': 20, 'timestamp': 3},
        {'serverID': 'server_1', 'CPU_Utilization': 30, 'timestamp': 2},
        {'serverID': 'server_2', 'CPU_Utilization': 40, 'timestamp': 3},
        {'serverID': 'server_3', 'CPU_Utilization': 50, 'timestamp': 6},
        {'serverID': 'server_1', 'CPU_Utilization': 60, 'timestamp': 7},
        {'serverID': 'server_2', 'CPU_Utilization': 70, 'timestamp': 7},
        {'serverID': 'server_3', 'CPU_Utilization': 80, 'timestamp': 14},
        {'serverID': 'server_2', 'CPU_Utilization': 85, 'timestamp': 8.5},
        {'serverID': 'server_1', 'CPU_Utilization': 90, 'timestamp': 14}
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

class ProcessPubSubDoFn(beam.DoFn):
  """parse pub/sub message."""
  def process(self, element):
      return element
    

#not used? 
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
        yield (element['serverID'], element)

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
        streaming=True, 
        save_main_session=True,
        template_location= known_args.template_location,
    )

    with Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline | 'Create Events' >> beam.Create(DATA) 
           #next line is needed in order to make field a timestamp
        | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) 
        | 'keyed on serverID' >> beam.ParDo(AddKeyInfoFn()) 
        | 'Add Window info' >>  beam.WindowInto(beam.window.Sessions(5)) 
        | 'Group by key' >> beam.GroupByKey() 
        | 'print1' >> beam.ParDo(PrintElements())
        )



if __name__ == "__main__":
    #logging.getLogger().setLevel(logging.INFO)

    run(
    )

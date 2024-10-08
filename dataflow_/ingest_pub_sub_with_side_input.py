import argparse
from datetime import datetime, date
import logging
import random
import json

from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import apache_beam as beam
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.io.fileio import WriteToFiles
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
            default = 'DataflowRunner',
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
        yield {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

class MergeWithSide(beam.DoFn):

    def __init__(self, side):
        self.side = side

    def process(self, element):
        # element[1] is the actual data, in binary
        d = json.loads(element[1][0])
        #v = self.side[0]
        x = beam.Map(lambda x: x)
        d['time'] = str(type(x))
        yield d

def  merge_with_side(element, side):
    d = json.loads(element[1][0])
    v = self.side
    d['time'] = v
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
    side = {'time':datetime(2024,9,29, 13)}

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
            | 'create data' >> beam.ParDo(CreateData())
            )
        
        main | ( 
             "merge" >> beam.ParDo(MergeWithSide(side = beam.pvalue.AsIter(side_input)))
            | "to json" >> beam.Map(dump_to_json)
            | "output to text" >>  WriteToFiles('gs://paul-henry-tremblay-general/ingest_pub_sub_with_sides10')
                

                )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    run(
    )

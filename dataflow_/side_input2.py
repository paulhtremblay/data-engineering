from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms import window
import apache_beam as beam
import datetime
import pprint
pp = pprint.PrettyPrinter(indent = 4)

from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.options.pipeline_options import PipelineOptions
import time
START = datetime.datetime.now() + datetime.timedelta(seconds = 0)

class CreateData(beam.DoFn):
    """
    example
    """

    def process(self, element):
        yield {'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

def join(element, side):
    final = {}
    for i in side:
        final[i.get('time')] = True
    return (element, list(final.keys()))

def print_func(element):
    pp.pprint(element)

def run():

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(
        options=pipeline_options, runner=beam.runners.DirectRunner()
    ) as p:
        side_input = (
            p
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=START.timestamp(),
                fire_interval=10,
                stop_timestamp=(START  + datetime.timedelta(seconds = 50)).timestamp(),
                apply_windowing=True,
            )
            | 'create data' >> beam.ParDo(CreateData())
        )
        main_input = (
            p
            | "PeriodicImpulse2"
            >> PeriodicImpulse(
                start_timestamp=START.timestamp(),
                stop_timestamp=(START  + datetime.timedelta(seconds = 30)).timestamp(),
                fire_interval=1,
                apply_windowing=True,
            )
            | 'create data2' >> beam.ParDo(CreateData())
        )

        result = (
            main_input
            | "Pair data" >> beam.Map(join, side=beam.pvalue.AsIter(side_input))
            | "filter" >> beam.Filter(lambda x: x is not None)
            | "print2" >> beam.Map(print_func)
        )
        #main_input | "pring3" >> beam.Map(print)



if __name__ == "__main__":
    run()

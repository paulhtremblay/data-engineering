from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms import window
import apache_beam as beam
import datetime

from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.options.pipeline_options import PipelineOptions
import time

class CreateData(beam.DoFn):
    """
    example
    """

    def process(self, element):
        yield {'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}


def run():

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(
        options=pipeline_options, runner=beam.runners.DirectRunner()
    ) as p:
        side_input = (
            p
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=datetime.datetime.now().timestamp(),
                fire_interval=10,
                apply_windowing=True,
            )
            | 'create data' >> beam.ParDo(CreateData())
            | 'print' >> beam.Map(print)
        )




if __name__ == "__main__":
    run()

import datetime

from apache_beam import Pipeline
import apache_beam as beam
DATA = [
        ['user1', 1],
        ['user2', 2],
    ]

DATA_= ['a', 'b']

class ExampleWithStartbundle(beam.DoFn):
    """
    example
    """

    def __init__(self, arg1):
        self.arg1 = arg1
        self.window = beam.transforms.window.GlobalWindow()

    def start_bundle(self, *args, **kwargs):
        self.elements = []

    def process(self, element, window = beam.DoFn.WindowParam):
        self.window = window
        yield element

    def setup(self):
        pass

    def finish_bundle(self, *args, **kwargs):
        yield beam.utils.windowed_value.WindowedValue(
            value ='Not sure what this does',
            timestamp = 0,
            windows = [self.window],
            )



def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | beam.ParDo(ExampleWithStartbundle(arg1 = 'foo'))
            | 'print' >> beam.ParDo(print)
                )
if __name__ == '__main__':
    run()

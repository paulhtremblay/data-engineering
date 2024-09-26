import datetime

from apache_beam import Pipeline
import apache_beam as beam
DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

class ExampleWithStartbundle(beam.DoFn):
    """
    example
    """

    def __init__(self, arg1):
        self.arg1 = arg1

    def start_bundle(self, *args, **kwargs):
        print('starting bundle')
        self.start_bundle = datetime.datetime.now()

    def process(self, element):
        yield (element, self.starttime, self.start_bundle)

    def setup(self):
        print('in setup')
        self.starttime = datetime.datetime.now()



def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | beam.ParDo(ExampleWithStartbundle(arg1 = 'foo'))
            | 'print' >> beam.ParDo(print)
                )
if __name__ == '__main__':
    run()

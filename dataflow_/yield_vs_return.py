"""
DoFn expects to return an Iterator. Using yield 
creates a generator, which is what is expected. 
If you wrap the result in a list, it will do the 
same thing, but probably best to always use yield
"""
from apache_beam import Pipeline
import apache_beam as beam
DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

class ReturnIt(beam.DoFn):
    def process(self, element):
        return [element]

class YieldIt(beam.DoFn):
    def process(self, element):
        return [element]

class ReturnItWrong(beam.DoFn):
    def process(self, element):
        return element


def run(type_):
    if type_ == 'return':
        c = ReturnIt
    elif type_ == 'yield':
        c = YieldIt
    elif type_ == 'return_wrong':
        c = ReturnItWrong
    else:
        raise ValueError('no value')

    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | 'yield' >> beam.ParDo(c())
            | 'print' >> beam.ParDo(print)
                )

if __name__ == '__main__':
    run('return')
    run('yield')
    run('return_wrong')

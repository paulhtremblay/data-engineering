from apache_beam import Pipeline
import apache_beam as beam
from typing import Tuple, List, Any, Iterable

DATA1 = [
        ('user1', 1),
        ('user2', 2),
    ]
def run():
    with Pipeline() as pipeline:
        lines1 = (pipeline | 'Create data1' >> beam.Create(DATA1) 
        | "filter" >> beam.Filter(lambda x: x[1] % 2 == 0).with_input_types(Tuple).
                  with_output_types(Tuple)
        | "print" >> beam.Map(print)
                )
if __name__ == '__main__':
    run()

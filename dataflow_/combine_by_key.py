from apache_beam import Pipeline
import apache_beam as beam
from typing import Tuple, List, Any, Iterable

DATA1 = [
        ('user1', 1),
        ('user2', 2),
    ]
DATA2 = [
        ('user1', ('a', 3)),
        ('user2', ('b', 4)),
    ]

def run():
    with Pipeline() as pipeline:
        lines1 = (pipeline | 'Create data1' >> beam.Create(DATA1) 
                )
        lines2 = (pipeline | 'Create data2' >> beam.Create(DATA2) 
                )
        ((lines1, lines2) 
         | "combine" >> beam.Flatten()
         | 'group by' >>  beam.GroupByKey()
         | "debug" >> beam.Map(print)
         )
if __name__ == '__main__':
    run()

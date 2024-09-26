
from apache_beam import Pipeline
import apache_beam as beam
DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

SIDE = [
        ('side', 3),
        ('side', 4),
        ]


class JoinData(beam.DoFn):
  """parse pub/sub message."""

  def __init__(self, right):
      self.right = right

  def process(self, element):
      return element

def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
                )
        side_input = ('create side' >> beam.Create(SIDE)
                      )
        final = (lines | 'join' >> beam.ParDo(JoinData(right = beam.pvalue.AsIter(side_input))
                                              )
                 )

if __name__ == '__main__':
    run()

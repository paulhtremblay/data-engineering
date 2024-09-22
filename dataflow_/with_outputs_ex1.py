from apache_beam import Pipeline
import apache_beam as beam
DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | "Split Messages" >> beam.ParDo(SplitMessages()).with_outputs(
                    'one', 'two')
                )
        (lines
            | 'print' >> beam.ParDo(print)
                )


class SplitMessages(beam.DoFn):
    def process(self, element):
        # third party libraries
        from apache_beam.pvalue import TaggedOutput
        if element[0] == 'user1':
            yield TaggedOutput('one', element)
            #yield ('one', element)
        else:
            yield TaggedOutput('two', element)

if __name__ == '__main__':
    run()

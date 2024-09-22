import traceback

from apache_beam import Pipeline
import apache_beam as beam

class CustomError(Exception):
    pass

DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def get_name(the_tuple):
    if the_tuple[0] == 'user1':
        return 'John'
    raise CustomError(f'no match for {the_tuple[0]}')

class GetName(beam.DoFn):

    def process(self, element):
        from apache_beam.pvalue import TaggedOutput
        try:
            name =  get_name(element)
            yield TaggedOutput('ok', name)
        except Exception as e:
            yield TaggedOutput('error', [traceback.format_exc(), element])


def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | "get name" >> beam.ParDo(GetName()).with_outputs(
                    'ok', 'error')
                )
        lines['error'] | "error" >> beam.ParDo(print)
if __name__ == '__main__':
    run()

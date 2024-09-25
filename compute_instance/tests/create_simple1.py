from apache_beam import Pipeline
import apache_beam as beam
DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
            | 'print' >> beam.ParDo(print)
                )
if __name__ == '__main__':
    run()

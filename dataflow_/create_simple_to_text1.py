import json
from apache_beam import Pipeline
import apache_beam as beam
from apache_beam.io.fileio import WriteToFiles

DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def dump_to_json(element):
    return json.dumps(element)

def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
        | "dump to json" >> beam.Map(dump_to_json)
        | WriteToFiles("output/create1_simple")
                )
if __name__ == '__main__':
    run()

from apache_beam import Pipeline
import apache_beam as beam
from typing import Tuple, List, Any, Iterable

DATA = [
        {'user': 'user1', 'type': 'with_place', 'name': 'Henry', 'place':'Tewksbury'},
        {'user': 'user1','type': 'with_no_place','name': 'Tom'},
        {'user': 'user1','type': 'with_no_place', 'name': 'Henry'},
         {'user': 'user2','type': 'with_place','name': 'Henry', 'place':'Tewksbury'},
         {'user': 'user2','type': 'with_no_place','name': 'Henry'},
    ]


class AddKey(beam.DoFn):


    def process(self, element):
        yield (element['user'], element)

class InsertPlace(beam.DoFn):
    """
    example
    """


    def process(self, element):
        place = None
        for i in element[1]:
            if i.get('place') != None:
                place = i['place']
                break
        for i in element[1]:
            i['place'] = place
            yield i


def run():
    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data1' >> beam.Create(DATA) 
        | "add key" >> beam.ParDo(AddKey())
         | 'group by' >>  beam.GroupByKey()
        | beam.ParDo(InsertPlace())
         | "debug" >> beam.Map(print)
                )
                
if __name__ == '__main__':
    run()

from unittest import mock

from typing import Union, Optional, Sequence, Type, TypeVar, Optional

from google.cloud import bigquery

class Row():

    def __init__(self, row):
        self.__info = {}
        l = []
        #self.bytes = 1
        self.__dict__['bytes'] = 1
        for i in row:
            self.__info[i.name] = i.value
            l.append(i.value)
        self.__values = tuple(l)
        self.row = row

    def get(self, *args, **kwargs):
        if args:
            return self.__info.get(args[0])
        return self.__info.get(kwargs['key'])

    def items(self, *args, **kwargs):
        for i in self.row:
            yield i.name, i.value

    def values(self, *args, **kwargs):
        return self.__values

    def keys(self, *args, **kwargs):
        return self.__info.keys()

class TempClass:

    def __init__(self, name, value):
        self.name = name
        self.value = value

class RowIterator:

    def __init__(self, data, m):
        def my_generator():
            for i in data:
                t = [TempClass(name = 'bytes', value = i)]
                yield t
        self.__data = my_generator()

    def __iter__(self):
        return self

    def __next__(self):
        v = next(self.__data)
        if not v:
            raise StopIteration
        else:
            return Row(row = v)

    def result(self):
        return self


class ClientM:

    def __init__(self, project:Union[str, None] = None, mock_data = None, 
            mock_list_of_tables = None):
        self.project = project
        self.__list_of_tables = mock_list_of_tables

    def query(self, query,
              ) :
        return self._query(data = [], m = {})

    def _query(self, data, m):
        return RowIterator(data =data, m = {})


class Client(ClientM):

    def query(self, query):
        return self._query(data = [1], m = {})


def mock_client(*args, **kwargs):
    return Client()

def get_mb(id_:int)-> int:
    client = bigquery.Client()
    sql = f"""
    SELECT  SUM(bytes) as bytes
FROM
  `paul-henry-tremblay.data_engineering.tv_streaming`
where id = {id_}
    """
    query_job = client.query(sql)
    rows = query_job.result()  # Waits for query to finish
    the_bytes= None
    for i in rows:
        the_bytes = i.bytes
        i.bytes
    if the_bytes == None:
        return 0
    return  .000001 * the_bytes

@mock.patch('google.cloud.bigquery.Client', side_effect= mock_client )
def test_get_mb1(m1):
    x = get_mb(1)
    print(x)


if __name__ == '__main__':
    #get_mb(1)
    test_get_mb1()
    # was to work with docker. That is too much? I think

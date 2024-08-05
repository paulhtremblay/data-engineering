from unittest import mock
from google.cloud import bigquery

def func1(arg1:int):
    return arg1

def mock_func(*args, **kwargs):
    pass


def main():
    print(get_mb(111))

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
    for i in rows:
        the_bytes = i.bytes

    if the_bytes == None:
        return 0
    return  .000001 * the_bytes

@mock.patch('google.cloud.bigquery.Client', side_effect= mock_func )
def main_mocked1(m1):
    print(func1(1))

@mock.patch('__main__.func1', side_effect= mock_func )
def main_mocked2(m1):
    print(func1(1))

@mock.patch('__main__.func1')
def main_func_mocked2(m1):
    print(get_mb(1))

@mock.patch('google.cloud.bigquery.Client', side_effect= mock_func )
def test_get_mb1(m1):
    print(get_mb(111))


if __name__ == '__main__':
    #main_func()
    #main_func_mocked1()
    main()
    test_get_mb1()

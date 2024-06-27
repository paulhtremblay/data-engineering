import time
import datetime
import os
from google.cloud import bigquery
from schemas import tv_streaming

import common
import get_sql

def insert_data(
        table_id, 
        client, 
        start_time = datetime.datetime(2024, 1, 1, 1, 1,1)):
    show_names = ['Tennis', 'Movies']
    bytes_ = [333, 455, 789, 888, 444, 333, 111, 222]
    data = {1: [1,3,5,1],
            2: [5],
            3: [1, 2],
                }
    d = []
    for key in data.keys():
        start_time_ = start_time
        for counter, row in enumerate(data[key]):
            for i in range(row):
                end_time = start_time_ + datetime.timedelta(seconds = 1)
                d.append(f"""({key}, 
                        \'{start_time_.strftime("%Y-%m-%d %H:%M:%S")}\', 
                        \'{end_time.strftime("%Y-%m-%d %H:%M:%S")}\',
                        \'{show_names[0]}\', 
                        {bytes_[i]})
                        """

                        )
                start_time_ += datetime.timedelta(seconds=1)
            start_time_ += datetime.timedelta(seconds=1)
    s = ',\n'.join(d)
    sql = f"""INSERT INTO 
    {table_id} (id, start_time, end_time,show_name, bytes) 
    VALUES
    {s}
    """
    result=  client.query(sql)
    result.result()


def main():
    project = 'paul-henry-tremblay'
    dataset_id = 'data_engineering'
    table_id = 'tv_streaming'
    client = bigquery.Client()
    test_query(client = client)
    return
    common.remove_table(table_id =  f'{project}.{dataset_id}.{table_id}',
            verbose = True, client = client,
            not_found_ok = True)
    common.create_table(
        client = client, 
        table_id = table_id, 
        dataset_id = dataset_id, 
        schema = tv_streaming.SCHEMA ,
        clustering_fields =  tv_streaming.CLUSTERING_FIELDS, 
        partition_field = None, 
        require_partition_filter = False, 
        description = None, 
        verbose = True)
    insert_data(
            table_id = f'{project}.{dataset_id}.{table_id}', 
            client = client)
    test_query(client = client)

def test_query(client):
    sql = get_sql.get_sql_string(path = 'sql/group_streaming.sql')
    result=  client.query(sql)
    result.result()
    stats = {}
    for i in result:
        d = {}
        for j in i.items():
            d[j[0]] = j[1]
        if not stats.get(d['id']):
            stats[d['id']] =  {}
        if not stats[d['id']].get(d['gn']):
            stats[d['id']][d['gn']] = {'n':0, 'start_time':None, 'end_time':None}


        stats[d['id']][d['gn']]['n'] += 1 
        stats[d['id']][d['gn']]['start_time'] =  d['group_start_time']
        stats[d['id']][d['gn']]['end_time'] =  d['group_end_time']

    def max_group(d, id_):
        return max(d[id_].keys())

    assert max_group(d = stats, id_ =1) == 4 #4 groups
    assert max_group(d = stats, id_ =2) == 1 #1 groups
    assert max_group(d = stats, id_ =3) == 2 #2 groups
    assert stats[1][1]['n'] == 1 #id 1, gn1 has 1 row
    assert stats[1][2]['n'] == 3 #id 1, gn2 has 3 row
    assert stats[1][3]['n'] == 5 #id 1, gn3 has 5 row
    assert stats[1][4]['n'] == 1 #id 1, gn4 has 1 row
    assert stats[3][1]['n'] == 1 #id 3, gn1 has 1 row
    assert stats[3][2]['n'] == 2 #id 3, gn2 has 2 row


if __name__ == '__main__':
    #'/home/henry/Downloads/paul-henry-tremblay-f45a9e9028d5.json'
    key_path = os.path.join('/', 'home', 
    'henry', 'Downloads', 'paul-henry-tremblay-f7082df82497.json')
    assert os.path.isfile(key_path)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  key_path
    main()

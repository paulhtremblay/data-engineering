import time
import datetime
import os
from google.cloud import bigquery
from schemas import tv_streaming

def remove_table(
        table_id, 
        client = None, 
        verbose = False,
        not_found_ok = False):
    if not client:
        client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=not_found_ok) 
    if verbose:
        print(f"Deleted table '{table_id}'.")

def insert_data(
        table_id, 
        client, 
        start_time = datetime.datetime(2024, 1, 1, 1, 1,1)):
    show_names = ['Tennis', 'Movies']
    bytes_ = [333, 455, 789, 888, 444, 333, 111, 222]
    data = {1: [1,3,5,1],
            2: [5],
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

def create_table(
        client, 
        table_id, 
        dataset_id, 
        schema, 
        clustering_fields =  None, 
        partition_field = None, 
        require_partition_filter = False, 
        use_partition = False,
        description = None, 
        verbose = False):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    if use_partition:
        if partition_field != None:
            table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field = partition_field, 
                    require_partition_filter = require_partition_filter)
        else:
            table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, 
                    require_partition_filter=require_partition_filter)
        
    if clustering_fields !=  None:
        table.clustering_fields = clustering_fields
    if description !=  None:
        table.description = description
    result = client.create_table(table)  
    if verbose:
        print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )


def main():
    project = 'paul-henry-tremblay'
    dataset_id = 'data_engineering'
    table_id = 'tv_streaming'
    client = bigquery.Client()
    remove_table(table_id =  f'{project}.{dataset_id}.{table_id}',
            verbose = True, client = client,
            not_found_ok = True)
    create_table(
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


if __name__ == '__main__':
    #'/home/henry/Downloads/paul-henry-tremblay-f45a9e9028d5.json'
    key_path = os.path.join('/', 'home', 
    'henry', 'Downloads', 'paul-henry-tremblay-f7082df82497.json')
    assert os.path.isfile(key_path)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  key_path
    main()

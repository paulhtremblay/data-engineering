import sys
import os
import datetime
import pprint
pp = pprint.PrettyPrinter(indent = 4)
from google.cloud import bigquery

def create_table(table_id,
        dataset_id, 
        schema, 
        client = None,
        clustering_fields =  None, 
        partition_field = None, 
        require_partition_filter = False, 
        verbose = False,
        description = None,
        ):
    if not client:
        client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    if partition_field != None: 
        table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field = partition_field, 
                require_partition_filter = require_partition_filter)
    elif partition_field == '_PARTITIONTIME':
        table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.TIME, 
                require_partition_filter=require_partition_filter)
    elif partition_field == '_PARTITIONDATE':
        table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, 
                require_partition_filter=require_partition_filter)
        
    if clustering_fields !=  None:
        table.clustering_fields = clustering_fields
    if description !=  None:
        table.description = description
    table = client.create_table(table)  
    for i in dir(table):
        if i.startswith('_'):
            continue
        print(i)

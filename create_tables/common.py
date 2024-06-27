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


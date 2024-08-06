from google.cloud import bigquery
SCHEMA=[ 
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('order_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('creative_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('placement_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('amt', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('sku', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('name1', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('name2', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('name3', 'STRING', mode='REQUIRED'),
        ]
DESCRIPTION = None
CLUSTERING_FIELDS = None
REQUIRE_PARTITION = True
PARTITION_FIELD = 'date'

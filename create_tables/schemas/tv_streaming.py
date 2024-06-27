from google.cloud import bigquery
SCHEMA = [ 
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('start_time', 'DATETIME', mode='REQUIRED'),
            bigquery.SchemaField('end_time', 'DATETIME', mode='REQUIRED'),
            bigquery.SchemaField('show_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('bytes', 'INTEGER', mode='REQUIRED'),


        ]
DESCRIPTION = None
CLUSTERING_FIELDS = None
REQUIRE_PARTITION = True
PARTITION_FIELD = 'start_time'


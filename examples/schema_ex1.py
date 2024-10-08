from google.cloud import bigquery
SCHEMA=[ 
        bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('platform', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('impressions', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('clicks', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('ctr', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('delivered_spend', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('average_cpc', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('click_units', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('click_revenue', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('click_roas', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('click_roas_percent', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('view_through_units', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('view_through_revenue', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('view_through_roas', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('view_through_roas_percent', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('click_and_view_units', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('click_and_view_revenue', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('click_and_view_roas', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('click_and_view_roas_percent', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('is_final', 'BOOLEAN', mode='NULLABLE'),
        ]
DESCRIPTION = None
CLUSTERING_FIELDS = None
REQUIRE_PARTITION = True
PARTITION_FIELD = 'date'

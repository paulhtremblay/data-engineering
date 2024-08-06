from google.cloud import bigquery

def load_table(uri, table_id):

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=0,
        source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(
        uri, table_id, 
        job_config=job_config
        )
    load_job.result()  
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

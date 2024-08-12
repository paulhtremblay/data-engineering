from google.cloud import bigquery

client = bigquery.Client()

QUERY = (
    'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
    'WHERE state = "TX" '
    'LIMIT 100')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    for j in row.items():
        print(j)
    i.name
    break

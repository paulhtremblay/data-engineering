from google.cloud import bigquery

client = bigquery.Client()
dataset_id = 'data_engineering'
tables = client.list_tables(dataset_id)  # Make an API request.

print("Tables contained in '{}':".format(dataset_id))
for table in tables:
    print(type(table))
    for i in dir(table):
        if i.startswith('_'):
            continue
        print (i)
    print(table.table_id)
    print(table.created)
    print(type(table.created))
    break
    print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))


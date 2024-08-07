from google.cloud import bigquery

def delete_table(
        table_id, 
        not_found_ok = True, 
        verbose = False):
    client = bigquery.Client()
    client.delete_table(table_id, 
            not_found_ok=not_found_ok)
    if verbose:
        print("Deleted table '{}'.".format(table_id))

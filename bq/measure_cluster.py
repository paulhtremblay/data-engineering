from google.cloud import bigquery
import statistics

def main():
    no_cluster = []
    cluster = []
    client = bigquery.Client(project="paul-henry-tremblay")
    for i in range(20):
        query_job = client.query("""
        SELECT sku,  RAND()
    FROM
      `paul-henry-tremblay.data_engineering.ads`
    WHERE
      date = "2024-08-05"
      and  order_id = 4000
      and creative_id = 2000
      and placement_id = 2000
        """)
        rows = query_job.result()  
        no_cluster.append(query_job.slot_millis)
    print(statistics.mean(no_cluster))

    for i in range(20):
        query_job = client.query("""
        SELECT sku,  RAND()
    FROM
      `paul-henry-tremblay.data_engineering.ads_with_cluster`
    WHERE
      date = "2024-08-05"
      and  order_id = 4000
      and creative_id = 2000
      and placement_id = 2000
        """)
        rows = query_job.result()  
        cluster.append(query_job.slot_millis)
    print(statistics.mean(cluster))

#table = client.get_table("data_engineering.ads_with_cluster")
#print(table.clustering_fields)

#table = client.get_table("data_engineering.ads_with_cluster")
#print(table.clustering_fields)

if __name__ == '__main__':
    main()



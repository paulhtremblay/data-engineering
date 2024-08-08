import os
import sys
import glob
import argparse
import shutil

import statistics

from google.cloud import bigquery
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


cur_dir = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
one_up = os.path.split(cur_dir)[0]
sys.path.append(one_up)

import examples.create_table
import examples.bq_load_table_from_uri
import examples.delete_table
import generate_ad_data
import examples.delete_table
import examples.upload_file_to_storage
import examples.delete_files_from_bucket
import ad_table_schema
import ad_table_schema_with_clustering

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("--measure", '-m',  action ='store_true',
            help = 'just measure')  
    parser.add_argument("--cardinality", '-c',  
            default = 5, type = int, help="cardinality of data")
    parser.add_argument("--num-files", '-n',  
            default = 5, type = int, help="cardinality of data")
    parser.add_argument("--sample-size", '-s',  
            default = 30, type = int, help="sample runs")
    parser.add_argument("--cols",   
             type = int, required = True,  help="num cols on benchmark")
    args = parser.parse_args()
    return args

def _make_hist(cluster, no_cluster, cardinality):
    plt.figure()
    sns.set_theme(style = 'white')
    df = pd.DataFrame(data = {'clust':cluster, 'no-clust':no_cluster})
    hist_plot = sns.histplot(data=df)
    hist_plot.set(xlabel='millis')
    hist_plot.set_title(f'Cluster vs No Cluster, Cardinality = {cardinality}')
    plt.savefig("benc_hist.png")

def _make_bar(cluster, no_cluster, cardinality):
    plt.figure()
    sns.set_theme(style = 'white')
    name = ['cluster', 'no-cluster']
    seconds = [statistics.mean(cluster), statistics.mean(no_cluster)]
    df = pd.DataFrame(data = {'type':name, 'millis': seconds})
    bar_plot = sns.barplot(df, x="type", y="millis")
    bar_plot.set_title(f'Cluster vs No Cluster, Cardinality = {cardinality}')
    plt.savefig('mean.png')
    plt.show()

def _print_stats(cluster, no_cluster, cardinality):
    print(statistics.mean(cluster)/statistics.mean(no_cluster))
    rat = round(statistics.mean(no_cluster)/statistics.mean(cluster), 1)
    print(f'diff is ({round(statistics.mean(no_cluster))} {round(statistics.mean(cluster))}) {rat}')
    return
    _make_hist(
            cluster = cluster, 
            no_cluster = no_cluster,
            cardinality = cardinality
            )
    _make_bar(
            cluster = cluster, 
            no_cluster = no_cluster,
            cardinality = cardinality
            )


def _run_query(client, sample_size, table_name, cols):
    l = []
    query = f"""
        SELECT sku,  RAND()
    FROM
     `{table_name}`
    WHERE
      date = "2024-08-05"
      and  order_id = 4000
        """
    if cols == 3:
        query += '\nand creative_id = 2000 \n AND placement_id = 2000'
    elif cols == 2:
        query += '\nand creative_id = 2000 '
    for i in range(sample_size):
        query_job = client.query(query)
        rows = query_job.result()  
        l.append(query_job.slot_millis)
    return l

def measure(sample_size, cols, verbose = False):
    client = bigquery.Client(project="paul-henry-tremblay")
    if verbose:
        print(f'running non cluster {sample_size} times')
    no_cluster = _run_query(
            sample_size = sample_size, 
            table_name = 'paul-henry-tremblay.data_engineering.ads',
            cols = cols,
            client = client
            )
    if verbose:
        print(f'running cluster {sample_size} times')
    cluster = _run_query(
            sample_size = sample_size, 
            table_name = 'paul-henry-tremblay.data_engineering.ads_with_cluster',
            cols = cols,
            client = client
            )
    return cluster, no_cluster

def main(num_files, 
        cardinality, 
        cols,
        sample_size = 30,
        verbose = False,
        just_measure = False):

    if not just_measure:
        shutil.rmtree('data')
        os.mkdir('data')
        examples.delete_files_from_bucket.delete_blob_with_pattern(
                prefix = 'ad_data',
                bucket_name= 'paul-henry-tremblay-general',
                verbose = verbose
                )
        generate_ad_data.main(
                num_files = num_files, 
                cardinality = cardinality, 
                verbose = verbose)
        for i in sorted(glob.glob('data/*')):
            dest = os.path.split(i)[1]
            examples.upload_file_to_storage.upload_blob(
                    bucket_name = 'paul-henry-tremblay-general',
                    source_file_name = i, 
                    destination_blob_name =  f'ad_data/{dest}',
                    verbose = verbose
                    )
        examples.delete_table.delete_table('paul-henry-tremblay.data_engineering.ads', 
                verbose = verbose)
        examples.create_table.create_table(
                table_id = 'ads', 
                dataset_id  = 'data_engineering',
                schema = ad_table_schema.SCHEMA,
                partition_field = ad_table_schema.PARTITION_FIELD,
                require_partition_filter = ad_table_schema.REQUIRE_PARTITION,
                verbose = verbose
                )
        examples.bq_load_table_from_uri.load_table(
            uri = "gs://paul-henry-tremblay-general/ad_data/*",
            table_id = 'paul-henry-tremblay.data_engineering.ads',
            verbose = verbose
                )
        examples.delete_table.delete_table(
                'paul-henry-tremblay.data_engineering.ads_with_cluster',
                verbose = verbose)
        examples.create_table.create_table(
                table_id = 'ads_with_cluster', 
                dataset_id  = 'data_engineering',
                schema = ad_table_schema_with_clustering.SCHEMA,
                partition_field = ad_table_schema_with_clustering.PARTITION_FIELD,
                require_partition_filter = ad_table_schema_with_clustering.REQUIRE_PARTITION,
                clustering_fields =  ad_table_schema_with_clustering.CLUSTERING_FIELDS, 
                verbose = verbose
                )
        examples.bq_load_table_from_uri.load_table(
            uri = "gs://paul-henry-tremblay-general/ad_data/*",
            table_id = 'paul-henry-tremblay.data_engineering.ads_with_cluster',
            verbose = verbose
                )
    cluster, no_cluster = measure(
            sample_size = sample_size, 
            cols = cols,
            verbose = verbose
            )
    _print_stats(
            cluster = cluster, 
            no_cluster = no_cluster,
            cardinality = cardinality)

if __name__ == '__main__':
    args = _get_args()
    main(
            cardinality = args.cardinality, 
            num_files = args.num_files,
            verbose = args.verbose,
            just_measure = args.measure,
            sample_size = args.sample_size,
            cols = args.cols
            )

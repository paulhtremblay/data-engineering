import os
import sys
import glob

cur_dir = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(cur_dir)

import examples.create_table
import examples.bq_load_table_from_uri
import examples.delete_table
import generate_ad_data
import examples.delete_table
import examples.upload_file_to_storage
import ad_table_schema
import ad_table_schema_with_clustering

def main():
    generate_ad_data.main(num_files = 100, cardinality = 5)
    for i in sorted(glob.glob('data/*')):
        dest = os.path.split(i)[1]
        examples.upload_file_to_storage.upload_blob(
                bucket_name = 'paul-henry-tremblay-general',
                source_file_name = i, 
                destination_blob_name =  f'ad_data/{dest}'
                )
    examples.delete_table.delete_table('paul-henry-tremblay.data_engineering.ads')
    examples.create_table.create_table(
            table_id = 'ads', 
            dataset_id  = 'data_engineering',
            schema = ad_table_schema.SCHEMA,
            partition_field = ad_table_schema.PARTITION_FIELD,
            require_partition_filter = ad_table_schema.REQUIRE_PARTITION,
            )
    examples.bq_load_table_from_uri.load_table(
        uri = "gs://paul-henry-tremblay-general/ad_data/*",
        table_id = 'paul-henry-tremblay.data_engineering.ads'
            )
    examples.delete_table.delete_table('paul-henry-tremblay.data_engineering.ads_with_cluster')
    examples.create_table.create_table(
            table_id = 'ads_with_cluster', 
            dataset_id  = 'data_engineering',
            schema = ad_table_schema_with_clustering.SCHEMA,
            partition_field = ad_table_schema_with_clustering.PARTITION_FIELD,
            require_partition_filter = ad_table_schema_with_clustering.REQUIRE_PARTITION,
            clustering_fields =  ad_table_schema_with_clustering.CLUSTERING_FIELDS, 
            )
    examples.bq_load_table_from_uri.load_table(
        uri = "gs://paul-henry-tremblay-general/ad_data/*",
        table_id = 'paul-henry-tremblay.data_engineering.ads_with_cluster'
            )

if __name__ == '__main__':
    main()

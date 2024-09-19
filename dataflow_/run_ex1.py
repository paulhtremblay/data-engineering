gcloud dataflow jobs run awacs_load_messages_v1 \
	--region 'us-central1'\
        --gcs-location gs://ad-tech-dev-dataflows-templates-prod/awacs_load_messages_v1 \
       --subnetwork 'https://www.googleapis.com/compute/v1/projects/bby-ad-tech-prod-7447/regions/us-central1/subnetworks/bby-network-s1' 

set -e
source envs.sh

gcloud dataflow flex-template run "getting-started-`date +%Y%m%d-%H%M%S`" \
 --template-file-gcs-location "gs://$BUCKET_NAME/getting_started-py.json" \
 --parameters output="gs://$BUCKET_NAME/output-" \
 --region "$REGION"

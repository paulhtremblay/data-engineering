set -e
source envs.sh
echo $FULL_TEMPLATE_NAME

gcloud dataflow flex-template run "logging-`date +%Y%m%d-%H%M%S`" \
 --template-file-gcs-location $FULL_TEMPLATE_NAME \
 --parameters verbosity=3 \
 --region "$REGION"

set -e
source envs.sh
echo $FULL_TEMPLATE_NAME

gcloud dataflow flex-template run "wordcount-flex-`date +%Y%m%d-%H%M%S`" \
 --template-file-gcs-location $FULL_TEMPLATE_NAME \
 --parameters input=gs://paul-henry-tremblay-general/kinglear.txt,output=gs://paul-henry-tremblay-general/flex1/words- \
 --region "$REGION"

set -e
source envs.sh

INPUT_FILE=gs://paul-henry-tremblay-general/kinglear.txt
OUTPUT_FILE=gs://paul-henry-tremblay-general/kinglear_out.txt
TEMP_LOCATION=gs://paul-henry-tremblay-general/temp
# paul-henry-tremblay-general/temp



gcloud dataflow jobs run test-wordcount \
	--gcs-location gs://paul-henry-tremblay-dataflow-templates/word_count_with_docker \
	--region us-central1 \
	--staging-location gs://paul-henry-tremblay-general/temp \
	--parameters input=gs://paul-henry-tremblay-general/kinglear.txt,output=gs://paul-henry-tremblay-general/kinglear_out.txt

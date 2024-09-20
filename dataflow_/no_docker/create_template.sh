set -e
source envs.sh

TEMP_LOCATION=gs://paul-henry-tremblay-general/temp


python  wordcount.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=$TEMP_LOCATION \
  --runner=DataflowRunner \
  --template_location  gs://paul-henry-tremblay-dataflow-templates/word_count_no_docker \
  --setup_file dependencies_/setup.py



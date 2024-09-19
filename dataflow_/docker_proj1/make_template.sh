set -e
source envs.sh

#INPUT_FILE=gs://paul-henry-tremblay-general/kinglear.txt
#OUTPUT_FILE=gs://paul-henry-tremblay-general/kinglear_out.txt
#TEMP_LOCATION=gs://paul-henry-tremblay-general/temp
TEMPLATE_LOCATION=gs://paul-henry-tremblay-dataflow-templates/docker_with_package



python  wordcount.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --runner=DataflowRunner \
  --experiments=use_runner_v2 \
  --sdk_container_image=$IMAGE_URL \
  --sdk_location=container \
  --template_location $TEMPLATE_LOCATION


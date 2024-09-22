set -e
source envs.sh

INPUT_FILE=gs://paul-henry-tremblay-general/kinglear.txt
OUTPUT_FILE=gs://paul-henry-tremblay-general/kinglear_out.txt
TEMP_LOCATION=gs://paul-henry-tremblay-general/temp

python  wordcount.py \
  --input=$INPUT_FILE \
  --output=$OUTPUT_FILE \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=$TEMP_LOCATION \
  --runner=DataflowRunner \
  --experiments=use_runner_v2 \
  --sdk_container_image=$IMAGE_URL \
  --sdk_location=container 


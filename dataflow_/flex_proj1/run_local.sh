set -e
source envs.sh

OUTPUT_FILE=gs://paul-henry-tremblay-general/flex_local/kinglear_out_flex-
INPUT_FILE=gs://paul-henry-tremblay-general/kinglear.txt
TEMP_LOCATION=gs://paul-henry-tremblay-general/temp

python  wordcount.py \
  --input=$INPUT_FILE \
  --output=$OUTPUT_FILE \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=$TEMP_LOCATION \
  --runner=DirectRunner \



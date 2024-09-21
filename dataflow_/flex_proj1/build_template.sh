set -e
source envs.sh

gcloud dataflow flex-template build $FULL_TEMPLATE_NAME \
  --image $IMAGE_URL \
  --sdk-language PYTHON \
  --metadata-file metadata.json 

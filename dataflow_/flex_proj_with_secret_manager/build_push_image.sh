set -e
source envs.sh

gcloud builds submit --tag $IMAGE_URL .

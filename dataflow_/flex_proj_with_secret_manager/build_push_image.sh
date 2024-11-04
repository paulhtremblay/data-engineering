set -e
source envs.sh

gcloud builds submit --tag $IMAGE_URL .

cp Dockerfile Dockerfile_in_place
mv Dockerfile_workder Dockerfile
gcloud builds submit --tag $IMAGE_URL_WORKER .
mv Dockerfile_in_place Dockerfile

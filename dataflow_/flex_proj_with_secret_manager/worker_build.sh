set -e
source envs.sh

cp Dockerfile Dockerfile_in_place
cp Dockerfile_worker Dockerfile
gcloud builds submit --tag $IMAGE_URL_WORKER .
mv Dockerfile_in_place Dockerfile

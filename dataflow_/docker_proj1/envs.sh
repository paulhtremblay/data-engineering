REGION=us-west1
REPOSITORY=dataflow-ex
PROJECT_ID=paul-henry-tremblay
FILE_NAME=d-image
TAG=tag1
IMAGE_URL=$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/dataflow/$FILE_NAME:$TAG
echo $IMAGE_URL

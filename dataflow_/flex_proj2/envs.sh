REGION=us-west1
PROJECT_ID=paul-henry-tremblay
REPOSITORY=dataflow-ex
FILE_NAME=logging-image
TAG=tag1
TEMPLATE_NAME=logging-py2.json
IMAGE_URL=$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/dataflow/$FILE_NAME:$TAG
BUCKET_NAME=paul-henry-tremblay-general
FULL_TEMPLATE_NAME=gs://$BUCKET_NAME/$TEMPLATE_NAME 


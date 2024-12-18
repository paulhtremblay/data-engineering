REGION=us-west1
PROJECT_ID=paul-henry-tremblay
REPOSITORY=dataflow-ex
FILE_NAME=secret-manager-image
FILE_NAME_WORKER=secret-manager-worker-image
TAG=tag1
TEMPLATE_NAME=secret-manager-py.json
IMAGE_URL=$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/dataflow/$FILE_NAME:$TAG
IMAGE_URL_WORKER=$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/dataflow/$FILE_NAME_WORKER:$TAG
BUCKET_NAME=paul-henry-tremblay-general
FULL_TEMPLATE_NAME=gs://$BUCKET_NAME/$TEMPLATE_NAME 

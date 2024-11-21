REGION=us-west1
REPOSITORY=dataflow-ex
PROJECT_ID=paul-henry-tremblay
FILE_NAME=linear-regression
TAG=tag1
TEMPLATE_NAME=linear-regression-py2.json
IMAGE_URL=$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/dataflow/$FILE_NAME:$TAG
BUCKET_NAME=paul-henry-tremblay-general
FULL_TEMPLATE_NAME=gs://$BUCKET_NAME/$TEMPLATE_NAME 


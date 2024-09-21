set -e
source envs.sh

gcloud dataflow flex-template build gs://$BUCKET_NAME/getting_started-py.json \
 --image-gcr-path "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/getting-started-python:latest" \
 --sdk-language "PYTHON" \
 --flex-template-base-image "PYTHON3" \
 --metadata-file "metadata.json" \
 --py-path "." \
 --env "FLEX_TEMPLATE_PYTHON_PY_FILE=getting_started.py" \
 --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"

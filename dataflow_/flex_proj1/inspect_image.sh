set -e
source envs.sh
docker pull $IMAGE_URL
#find the image  with docker images; get the IMAGE ID
#docker inspect IMAGE ID

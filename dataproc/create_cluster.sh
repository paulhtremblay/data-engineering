set -e


gcloud dataproc clusters create test-cluster7 --enable-component-gateway --region us-west2 --subnet dataproc-sub --master-machine-type n2-standard-4 --master-boot-disk-type pd-balanced --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n2-standard-4 --worker-boot-disk-type pd-balanced --worker-boot-disk-size 500 --image-version 2.2-debian12 --tags dataproc-network --project paul-henry-tremblay \
 --public-ip-address \
 --zone=""


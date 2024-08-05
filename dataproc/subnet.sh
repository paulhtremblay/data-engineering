gcloud compute firewall-rules create allow-internal-ingress \
    --network=test-network \
    --source-ranges=SUBNET_RANGES \
    --destination-ranges=SUBNET_RANGES \
    --direction=ingress \
    --action=allow \
    --rules=all

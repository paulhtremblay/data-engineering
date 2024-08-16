#-----------------------------------------------------------#
# docker compose up -d
# to tear down:
# docker compose down
#-----------------------------------------------------------#




#make sure you get right id by
#docker ps
# there will be 2
CONTAINER_ID=62f4ce92221b

docker exec -it $CONTAINER_ID /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic my-topic

# do this in pyton 
#docker exec -it $CONTAINER_ID /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic
#docker exec -it $CONTAINER_ID /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning


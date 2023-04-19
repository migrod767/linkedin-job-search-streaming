# Setup Comands

## Kafka
Execute kafka container with container id given above

docker exec -it 1c31511ce206 bash

Create Kafka "odometry" topic for ROS odom data

kafka$ bin/kafka-topics.sh --create --topic odom --partitions 1 --replication-factor 1 -bootstrap-server localhost:9092

bin/kafka-topics.sh --list --bootstrap-server localhost:9092

go to **/opt/bitnami/kafka/bin** 
### Create Topic
./kafka-topics.sh --create --topic linkedin_scrapper --bootstrap-server localhost:9092

### List Topics
./opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

### Describe a topic
./opt/bitnami/kafka/bin/kafka-topics.sh --describe --topic linkedin_scrapper --bootstrap-server localhost:9092

./opt/bitnami/kafka/bin/kafka-console-consumer.sh --topic linkedin_scrapper --bootstrap-server localhost:9092

### Check Consumer Offset
./kafka-consumer-groups.sh --bootstrap-server localhost:9092  --describe --group mypythonconsumer

## Zookeeper

Execute zookeeper contcainer with container id given above

docker exec -it 1c31511ce206 bash

run command

opt/bitnami/zookeeper/bin/zkCli.sh -server localhost:2181

list all brokers topic

ls /brokers/topics

## Cassandra




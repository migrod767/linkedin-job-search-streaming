# Setup Commands

## Kafka
Run kafka container shell

```docker exec -it 1c31511ce206 bash```

go to ``` cd /opt/bitnami/kafka/bin```

### Create Topic
```./kafka-topics.sh --create --topic linkedin_scrapper --bootstrap-server localhost:9092```

### List Topics
```./kafka-topics.sh --list --bootstrap-server localhost:9092```

### Describe a Topic
```./kafka-topics.sh --describe --topic linkedin_scrapper --bootstrap-server localhost:9092```

### Create Consumer
```./kafka-console-consumer.sh --topic linkedin_scrapper --bootstrap-server localhost:9092```

### Check Consumer Offset
```./kafka-consumer-groups.sh --bootstrap-server localhost:9092  --describe --group mypythonconsumer```

## Zookeeper

Run zookeeper container shell

```docker exec -it 1c31511ce206 bash```

run command

```opt/bitnami/zookeeper/bin/zkCli.sh -server localhost:2181```

### List all brokers topic

```ls /brokers/topics```





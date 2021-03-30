# kafka-sample


Simple REST API which can read a batch of messages from a topic related to a consumer group id. 




## Usage

```python


Start server : (mac)
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties

Post message to topic : 
kafka-console-producer --broker-list localhost:9092 --topic <topic name>

Read as a consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic name> --from-beginning

Run the spring boot jar: (in dev)
mvn spring-boot:run -Dspring.profiles.active=dev
```


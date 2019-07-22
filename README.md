KafkaOffsetTool is a lightweight tool for Kafka offset operation and maintenance

## Quick start

#### Development environment installation
```
cd ${PROJECT_HOME}
go run pkg/kafka_offset_tool.go --action list-group --brokers localhost:9092
```

#### Production environment installation
- [Windows](scripts/build.bat)
- [Linux](scripts/build.sh)

#### Using of commands(Supporting two types of consumer operations: zookeeper and Kafka).
- All command help. e.g
```
./kafkaOffsetTool --help
```

- Sub-command help. e.g
```
./kafkaOffsetTool list-group --help
```

- Get a list of all topics. e.g.
```
./kafkaOffsetTool list-topic --brokers=localhost:9092 --zkServers=localhost:2181
```

- Get a list of all consumer group. e.g.
```
./kafkaOffsetTool list-group --brokers=localhost:9092 --zkServers=localhost:2181 --type=kf
```

- Get a list of all group consumer owner offset describe. e.g.
```
./kafkaOffsetTool list-offset --brokers=localhost:9092 --zkServers=localhost:2181
```

- Get a list of all group consumer owner offset describe export to file. e.g.
```
./kafkaOffsetTool list-offset --brokers=localhost:9092 --zkServers=localhost:2181 --exportFile=myTopic_offset.json
```

- Reset the specified groupId, topic, and partition offset. e.g.
```
./kafkaOffsetTool reset-offset --brokers=localhost:9092 --zkServers=localhost:2181 --resetGroup=myConsumerGroup1 --resetTopic=mytopic1 --resetPartition=0 --resetOffset=100
```

- Reset the specified groupId, topic, and partition offset from import file. e.g.
```
./kafkaOffsetTool reset-offset --brokers=localhost:9092 --zkServers=localhost:2181 --importFile=myTopic_offset.json
```

#### Welcome to raise bugs:
Contact author: <wanglsir@gmail.com,983708408@qq.com>
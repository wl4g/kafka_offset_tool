KafkaOffsetTool is a lightweight tool for Kafka offset operation and maintenance

[![Build Status](https://travis-ci.org/wl4g/kafka_offset_tool.svg?branch=master)](https://travis-ci.org/wl4g/kafka_offset_tool.svg)
![License](https://camo.githubusercontent.com/ce4fb5b3ec026da9d76d9de28d688d0a0d493949/68747470733a2f2f696d672e736869656c64732e696f2f6769746875622f6c6963656e73652f73706f746966792f646f636b657266696c652d6d6176656e2e737667)

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

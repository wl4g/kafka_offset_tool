# KafkaOffsetTool is a lightweight tool for Kafka offset operation and maintenance

[![Build Status](https://travis-ci.org/wl4g/kafka_offset_tool.svg?branch=master)](https://travis-ci.org/wl4g/kafka_offset_tool.svg)
![License](https://camo.githubusercontent.com/ce4fb5b3ec026da9d76d9de28d688d0a0d493949/68747470733a2f2f696d672e736869656c64732e696f2f6769746875622f6c6963656e73652f73706f746966792f646f636b657266696c652d6d6176656e2e737667)

## Quick start

### Developer guide

- a. Import to VSCode

- b. Run -> Start Debugging

### Compiling installation

- [Linux](scripts/build.sh)

```bash
./scripts/build.sh
```

- [Windows](scripts/build.bat)

```dos
./scripts/build.bat
```

### Command Usages

- All command help

```bash
./kafkaOffsetTool --help
```

- Sub-command help.

```bash
./kafkaOffsetTool get-group --help
```

- Get a list of consumer group.

```bash
./kafkaOffsetTool get-group
# or
./kafkaOffsetTool get-group --brokers=localhost:9092 --zkServers=localhost:2181 --type=kf --filter='(^spark\S+)'
```

- Get a list of topics.

```bash
./kafkaOffsetTool get-topic
# or
./kafkaOffsetTool get-topic --brokers=localhost:9092 --zkServers=localhost:2181 --filter='(^elecpower\S+)'
```

- Get a list of group consumer owner offset describe.

```bash
./kafkaOffsetTool get-offset
# or
./kafkaOffsetTool get-offset --brokers=localhost:9092 --zkServers=localhost:2181 --outputFile=myoffset.json --groupFilter='(^console\S+)' --topicFilter='(^elecpower\S+)'
```

- Set the specified groupId, topic, and partition offset.

```bash
./kafkaOffsetTool set-offset --brokers=localhost:9092 --zkServers=localhost:2181 --resetGroup=myConsumerGroup1 --setTopic=mytopic1 --setPartition=0 --setOffset=100
# or, Set the specified groupId, topic, and partition offset from import file.
./kafkaOffsetTool set-offset --brokers=localhost:9092 --zkServers=localhost:2181 --inputFile=myoffset.json
```

- Modify offset calculator tool.

```bash
./kafkaOffsetTool offset-calc --inputFile=myoffset.json --outputFile=myoffset2.json --increment -1000
```

### Welcome reporting bugs

Contact author: <wanglsir@gmail.com,983708408@qq.com>

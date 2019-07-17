KafkaOffsetTool a lightweight Tool for Kafka Migration Operation and Maintenance

## Quick start

#### Development environment installation
```
cd ${PROJECT_HOME}
go run pkg/kafka_offset_tool.go --action list-group --brokers localhost:9092
```

#### Production environment installation
- [Windows](scripts/build.bat)
- [Linux](scripts/build.sh)
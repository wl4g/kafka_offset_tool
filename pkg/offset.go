/**
 * Copyright 2017 ~ 2025 the original author or authors[983708408@qq.com].
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this export except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wl4g/kafka_offset_tool/pkg/common"
)

/**
 * Reset topic group partitions offset.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func setOffset() {
	log.Printf("Checking reset offset infor...")

	// Check reset offset range of consumer group/topic/partition.
	fetchedGroupConsumedOffsets := analysisConsumedTopicPartitionOffsets("*")

	if !common.IsBlank(option.inputFile) {
		log.Printf("Import file reset offset from %s ...", option.inputFile)
		setGroupConsumedOffsets := GroupConsumedOffsets{}
		common.ParseJSONFromFile(option.inputFile, &setGroupConsumedOffsets)

		for setGroup, consumedTopicOffset := range setGroupConsumedOffsets {
			for setTopic, partitionOffset := range consumedTopicOffset {
				for setPartition, setConsumedOffset := range partitionOffset {
					log.Printf("Batch import reset offset for %s/%s/%d/%d...", setGroup, setTopic,
						setPartition, setConsumedOffset.ConsumedOffset)
					doSetOffset(fetchedGroupConsumedOffsets, setGroup, setTopic, int64(setPartition),
						setConsumedOffset.ConsumedOffset)
				}
			}
		}
	} else {
		log.Printf("Simple reset offset for %s/%s/%d/%d...", option.setGroupId, option.setTopic, option.setPartition, option.setOffset)
		doSetOffset(fetchedGroupConsumedOffsets, option.setGroupId, option.setTopic, option.setPartition, option.setOffset)
	}
}

/**
 * Do reset(kafka or zookeeper) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func doSetOffset(fetchedGroupConsumedOffsets GroupConsumedOffsets, setGroupId string, setTopic string, setPartition int64, setOffset int64) {
	match := false
	for groupId, topicPartitionOffsets := range fetchedGroupConsumedOffsets {
		if groupId == setGroupId {
			for topic, partitionOffsets := range topicPartitionOffsets {
				if topic == setTopic {
					for partition, consumedOffset := range partitionOffsets {
						if int64(partition) == setPartition {
							match = true
							if setOffset <= consumedOffset.OldestOffset || setOffset >= consumedOffset.ConsumedOffset {
								common.FatalExit("Failed to reset offset, must be between %d and %d, %s/%s/%d/%d",
									consumedOffset.OldestOffset, consumedOffset.ConsumedOffset, setGroupId, setTopic, setPartition,
									setOffset)
							}
							break
						}
					}
				}
			}
		}
	}
	if !match { // Invalid group,topic,partition ?
		common.FatalExit("Failed to set offset, because no matchs setting group: %s, topic: %s, partition: %s",
			setGroupId, setTopic, setPartition)
	}

	// Check if the consumer type of the group is KAFKA direct(not zookeeper)?
	isKafkaDirectConsumerGroup := false
	for _, broker := range listBrokers() {
		groupIds := listKafkaGroupId(broker)
		if common.StringsContains(groupIds, setGroupId, false) {
			isKafkaDirectConsumerGroup = true
			break
		}
	}

	// Do reset specific offset.
	if isKafkaDirectConsumerGroup {
		doSetKafkaOffset(setGroupId, setTopic, setPartition, setOffset)
	} else {
		doSetZookeeperOffset(setGroupId, setTopic, setPartition, setOffset)
	}
}

/**
 * Reset(kafka) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func doSetKafkaOffset(setGroupId string, setTopic string, setPartition int64, setOffset int64) {
	// Handle reset offset.
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(setGroupId, option.client)
	var pom, _ = offsetManager.ManagePartition(setTopic, int32(setPartition))

	// Do reset offset.
	log.Printf("Resetting offset via kafka direct...")
	pom.ResetOffset(int64(setOffset), "modified_meta")

	// Sleep 1s, because the reset may not have been committed.
	time.Sleep(2 * time.Second)
	defer pom.Close()

	log.Printf("Reset kafka direct offset(%d) for group(%s), topic(%s), partition(%d) completed!",
		setOffset, setTopic, setGroupId, setPartition)
}

/**
 * Reset(zookeeper) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func doSetZookeeperOffset(setGroupId string, setTopic string, setPartition int64, setOffset int64) {
	log.Printf("Checking reset zookeeper offset range of group/topic/partition...")

	// Get reset consumer group.
	setConsumerGroupId := option.zkClient.Consumergroup(setGroupId)
	if *&setConsumerGroupId == nil {
		common.FatalExit("Failed to set offset, because not exist groupId of (%s)", setGroupId)
	}

	// Do set new offset.
	if err := setConsumerGroupId.CommitOffset(setTopic, int32(setPartition), setOffset); err == nil {
		log.Printf("Reset zookeeper offset(%d) for topic(%s), group(%s), partition(%d) successfuly!",
			setOffset, setTopic, setGroupId, setPartition)
	} else {
		common.ErrorExit(err, "Failed to set zookeeper offset(%d) for topic(%s), group(%s), partition(%d)",
			setOffset, setTopic, setGroupId, setPartition)
	}
}

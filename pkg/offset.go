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
					doSetOffset(fetchedGroupConsumedOffsets, setGroup, setTopic, int64(setPartition),
						setConsumedOffset.ConsumedOffset)
				}
			}
		}
	} else {
		log.Printf("Simple set offset for %s, %s, %d, %d ...", option.setGroupId, option.setTopic, option.setPartition, option.setOffset)
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
	matched := false
	for groupId, topicPartitionOffsets := range fetchedGroupConsumedOffsets {
		if groupId == setGroupId {
			for topic, partitionOffsets := range topicPartitionOffsets {
				if topic == setTopic {
					for partition, consumedOffset := range partitionOffsets {
						if int64(partition) == setPartition {
							// Check valid?
							if setOffset >= consumedOffset.OldestOffset && setOffset <= consumedOffset.ConsumedOffset {
								matched = true
							} else { // Invalid
								common.Warning("Invalid set offsets, must be between %d and %d of setGroupId: %s, setTopic: %s, setPartition: %d, setOffset: %d",
									consumedOffset.OldestOffset, consumedOffset.ConsumedOffset, setGroupId, setTopic, setPartition, setOffset)
							}
							break
						}
					}
				}
			}
		}
	}

	if matched { // Matched group,topic,partition and valid
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
	} else {
		common.Warning("Failed to set offset, because no matchs setting group: %s, topic: %s, partition: %d",
			setGroupId, setTopic, setPartition)
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
	var offsetManager, err1 = sarama.NewOffsetManagerFromClient(setGroupId, option.client)
	var pom, err2 = offsetManager.ManagePartition(setTopic, int32(setPartition))
	defer pom.Close()
	if err1 != nil || err2 != nil {
		common.Warning("Failed to set kafka offset(%d) for group(%s), topic(%s), partition(%d). - err1: %v, err2: %v",
			setOffset, setTopic, setGroupId, setPartition, err1, err2)
		return
	}

	// Do reset offset.
	log.Printf("Resetting offset via kafka direct...")
	pom.ResetOffset(int64(setOffset), "modified_meta")

	// Sleep 1s, because the reset may not have been committed.
	time.Sleep(2 * time.Second)

	log.Printf("Seted kafka direct offset(%d) for group(%s), topic(%s), partition(%d) completed!",
		setOffset, setTopic, setGroupId, setPartition)
}

/**
 * Reset(zookeeper) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func doSetZookeeperOffset(setGroupId string, setTopic string, setPartition int64, setOffset int64) {
	log.Printf("Preparing set zk offset range of group: %s, topic: %s, partition: %d, offset: %d ...",
		setGroupId, setTopic, setPartition, setOffset)

	// Get reset consumer group.
	setConsumerGroupId := option.zkClient.Consumergroup(setGroupId)
	if setConsumerGroupId != nil {
		// Check current offset and setOffset
		fetchedOffset, err1 := setConsumerGroupId.FetchOffset(setTopic, int32(setPartition))
		if err1 == nil {
			if setOffset != fetchedOffset {
				// Do set new offset.
				if err2 := setConsumerGroupId.CommitOffset(setTopic, int32(setPartition), setOffset); err2 == nil {
					log.Printf("Seted zk offset(%d) for group(%s), topic(%s), partition(%d) successful",
						setOffset, setGroupId, setTopic, setPartition)
				} else {
					common.ErrorExit(err2, "Failed to set zk offset(%d) for topic(%s), group(%s), partition(%d)",
						setOffset, setTopic, setGroupId, setPartition)
				}
			} else {
				log.Printf("Already a set offset nothing todo. group: %s, topic: %s, partition: %d, offset: %d",
					setGroupId, setTopic, setPartition, setOffset)
			}
		}
	} else {
		common.Warning("Failed to set offset, because not exist groupId of (%s)", setGroupId)
	}
}

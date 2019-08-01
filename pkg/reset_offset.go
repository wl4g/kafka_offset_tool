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
	"github.com/Shopify/sarama"
	"github.com/wl4g/kafka_offset_tool/pkg/common"
	"log"
	"time"
)

/**
 * Reset topic group partitions offset.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func resetOffset() {
	log.Printf("Checking reset offset infor...")

	// Check reset offset range of consumer group/topic/partition.
	_groupConsumedOffsets := analysisConsumedTopicPartitionOffsets("*")

	if !common.IsBlank(opt.importFile) {
		resetGroupConsumedOffsets := GroupConsumedOffsets{}
		common.ParseJSONFromFile(opt.importFile, &resetGroupConsumedOffsets)

		for _resetGroup, consumedTopicOffset := range resetGroupConsumedOffsets {
			for _resetTopic, partitionOffset := range consumedTopicOffset {
				for _resetPartition, _resetConsumedOffset := range partitionOffset {
					log.Printf("Batch import reset offset for %s/%s/%d/%d...", _resetGroup, _resetTopic,
						_resetPartition, _resetConsumedOffset.ConsumedOffset)
					doResetOffset(_groupConsumedOffsets, _resetGroup, _resetTopic, int64(_resetPartition),
						_resetConsumedOffset.ConsumedOffset)
				}
			}
		}
	} else {
		log.Printf("Simple reset offset for %s/%s/%d/%d...", opt.resetGroupId, opt.resetTopic, opt.resetPartition, opt.resetOffset)
		doResetOffset(_groupConsumedOffsets, opt.resetGroupId, opt.resetTopic, opt.resetPartition, opt.resetOffset)
	}
}

/**
 * Do reset(kafka or zookeeper) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func doResetOffset(groupConsumedOffsets GroupConsumedOffsets, resetGroupId string, resetTopic string, resetPartition int64, resetOffset int64) {
	match := false
	for groupId, topicPartitionOffsets := range groupConsumedOffsets {
		if groupId == resetGroupId {
			for topic, partitionOffsets := range topicPartitionOffsets {
				if topic == resetTopic {
					for partition, consumedOffset := range partitionOffsets {
						if int64(partition) == resetPartition {
							match = true
							if resetOffset <= consumedOffset.OldestOffset || resetOffset >= consumedOffset.ConsumedOffset {
								common.FatalExit("Failed to reset offset, must be between %d and %d, %s/%s/%d/%d",
									consumedOffset.OldestOffset, consumedOffset.ConsumedOffset, resetGroupId, resetTopic, resetPartition,
									resetOffset)
							}
							break
						}
					}
				}
			}
		}
	}
	if !match { // Invalid group,topic,partition ?
		common.FatalExit("Failed to reset offset, group(%s), topic(%s), partition(%s)",
			resetGroupId, resetTopic, resetPartition)
	}

	// Check if the consumer type of the group is KAFKA direct(not zookeeper)?
	isKafkaDirectConsumerGroup := false
	for _, broker := range listBrokers() {
		groupIds := listKafkaGroupId(broker)
		if common.StringsContains(groupIds, resetGroupId, false) {
			isKafkaDirectConsumerGroup = true
			break
		}
	}

	// Do reset specific offset.
	if isKafkaDirectConsumerGroup {
		doResetKafkaOffset(resetGroupId, resetTopic, resetPartition, resetOffset)
	} else {
		doResetZookeeperOffset(resetGroupId, resetTopic, resetPartition, resetOffset)
	}
}

/**
 * Reset(kafka) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func doResetKafkaOffset(resetGroupId string, resetTopic string, resetPartition int64, resetOffset int64) {
	// Handle reset offset.
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(resetGroupId, opt.client)
	var pom, _ = offsetManager.ManagePartition(resetTopic, int32(resetPartition))

	// Do reset offset.
	log.Printf("Resetting offset via kafka direct...")
	pom.ResetOffset(int64(resetOffset), "modified_meta")

	// Sleep 1s, because the reset may not have been committed.
	time.Sleep(2 * time.Second)
	defer pom.Close()

	log.Printf("Reset kafka direct offset(%d) for group(%s), topic(%s), partition(%d) completed!",
		resetOffset, resetTopic, resetGroupId, resetPartition)
}

/**
 * Reset(zookeeper) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func doResetZookeeperOffset(resetGroupId string, resetTopic string, resetPartition int64, resetOffset int64) {
	log.Printf("Checking reset zookeeper offset range of group/topic/partition...")

	// Get reset consumer group.
	_resetConsumerGroupId := opt.zkClient.Consumergroup(resetGroupId)
	if *&_resetConsumerGroupId == nil {
		common.FatalExit("Failed to reset offset, not exist groupId of (%s)", resetGroupId)
	}

	// Do reset offset.
	if err := _resetConsumerGroupId.CommitOffset(resetTopic, int32(resetPartition), resetOffset); err == nil {
		log.Printf("Reset zookeeper offset(%d) for topic(%s), group(%s), partition(%d) successfuly!",
			resetOffset, resetTopic, resetGroupId, resetPartition)
	} else {
		common.ErrorExit(err, "Failed to reset zookeeper offset(%d) for topic(%s), group(%s), partition(%d)",
			resetOffset, resetTopic, resetGroupId, resetPartition)
	}
}

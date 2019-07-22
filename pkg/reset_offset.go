/**
 * Copyright 2017 ~ 2025 the original author or authors[983708408@qq.com].
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
	"kafka_offset_tool/pkg/common"
	"log"
	"time"
)

/**
 * Reset topic group partitions offset.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func resetOffset() {
	log.Printf("Checking reset offset range of group/topic/partition...")

	// Check reset offset range of consumer group/topic/partition.
	match := false
	for groupId, topicPartitionOffsets := range analysisConsumedTopicPartitionOffsets() {
		if groupId == opt.resetGroupId {
			for topic, partitionOffsets := range topicPartitionOffsets {
				if topic == opt.resetTopic {
					for partition, consumedOffset := range partitionOffsets {
						if int64(partition) == opt.resetPartition {
							match = true
							if opt.resetOffset <= consumedOffset.OldestOffset || opt.resetOffset >= consumedOffset.ConsumedOffset {
								common.FatalExit("Failed to reset offset, must be between %d and %d",
									consumedOffset.OldestOffset, consumedOffset.ConsumedOffset)
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
			opt.resetGroupId, opt.resetTopic, opt.resetPartition)
	}

	// Check if the consumer type of the group is KAFKA direct(not zookeeper)?
	isKafkaDirectConsumerGroup := false
	for _, broker := range listBrokers() {
		groupIds := listKafkaGroupId(broker)
		if common.StringsContains(groupIds, opt.resetGroupId) {
			isKafkaDirectConsumerGroup = true
			break
		}
	}

	// Do resetting offset.
	if isKafkaDirectConsumerGroup {
		doResetKafkaOffset()
	} else {
		doResetZookeeperOffset()
	}
}

/**
 * Reset(kafka) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func doResetKafkaOffset() {
	// Handle reset offset.
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(opt.resetGroupId, opt.client)
	var pom, _ = offsetManager.ManagePartition(opt.resetTopic, int32(opt.resetPartition))

	// Do reset offset.
	log.Printf("Resetting offset via kafka direct...")
	pom.ResetOffset(int64(opt.resetOffset), "modified_meta")

	// Sleep 1s, because the reset may not have been committed.
	time.Sleep(2 * time.Second)
	defer pom.Close()

	log.Printf("Reset kafka direct offset(%d) for group(%s), topic(%s), partition(%d) completed!",
		opt.resetOffset, opt.resetTopic, opt.resetGroupId, opt.resetPartition)
}

/**
 * Reset(zookeeper) topic group partitions offset.
 * See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func doResetZookeeperOffset() {
	log.Printf("Checking reset zookeeper offset range of group/topic/partition...")

	// Get reset consumer group.
	_resetConsumerGroupId := opt.zkClient.Consumergroup(opt.resetGroupId)
	if &_resetConsumerGroupId == nil {
		common.FatalExit("Failed to reset offset, not exist groupId of (%s)", opt.resetGroupId)
	}

	// Do reset offset.
	if err := _resetConsumerGroupId.CommitOffset(opt.resetTopic, int32(opt.resetPartition), opt.resetOffset); err == nil {
		log.Printf("Reset zookeeper offset(%d) for topic(%s), group(%s), partition(%d) successfuly!",
			opt.resetOffset, opt.resetTopic, opt.resetGroupId, opt.resetPartition)
	} else {
		common.ErrorExit(err, "Failed to reset zookeeper offset(%d) for topic(%s), group(%s), partition(%d)",
			opt.resetOffset, opt.resetTopic, opt.resetGroupId, opt.resetPartition)
	}
}

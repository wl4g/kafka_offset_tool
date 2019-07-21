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
	"kafka_offset_tool/pkg/tool"
	"log"
	"time"
)

// Reset topic group partitions offset.
func resetOffset() {
	// Check if the consumer type of the group is KAFKA direct(not zookeeper)?
	isKafkaDirectConsumerGroup := false
	for _, broker := range listBrokers() {
		groupIds := listKafkaGroupId(broker)
		if tool.StringsContains(groupIds, opt.resetGroupId) {
			isKafkaDirectConsumerGroup = true
			break
		}
	}

	if isKafkaDirectConsumerGroup {
		resetKafkaOffset()
	} else {
		resetZookeeperOffset()
	}
}

// Reset(kafka) topic group partitions offset.
// See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
func resetKafkaOffset() {
	// Check reset offset range.
	var _oldestOffset int64
	var _consumedOffset int64
	for groupId, topicPartitionOffsets := range analysisConsumedTopicPartitionOffsets() {
		if groupId == opt.resetGroupId {
			for topic, partitionOffsets := range topicPartitionOffsets {
				if topic == opt.resetTopic {
					for partition, consumedOffset := range partitionOffsets {
						if int64(partition) == opt.resetPartition {
							_oldestOffset = consumedOffset.OldestOffset
							_consumedOffset = consumedOffset.ConsumedOffset
							break
						}
					}
				}
			}
		}
	}
	if opt.resetOffset <= _oldestOffset || opt.resetOffset >= _consumedOffset {
		tool.FatalExit("Failed to reset offset, must be between %d and %d", _oldestOffset, _consumedOffset)
	}

	// Handle reset offset.
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(opt.resetGroupId, opt.client)
	var pom, _ = offsetManager.ManagePartition(opt.resetTopic, int32(opt.resetPartition))

	log.Printf("Resetting offset via kafka direct...")
	pom.ResetOffset(int64(opt.resetOffset), "modified_meta")

	// Sleep 1s, because the reset may not have been submitted
	time.Sleep(2 * time.Second)
	defer pom.Close()

	log.Printf("Reset kafka direct offset(%d) for group(%s), topic(%s), partition(%d) completed!",
		opt.resetOffset, opt.resetTopic, opt.resetGroupId, opt.resetPartition)
}

// Reset(zk) topic group partitions offset.
func resetZookeeperOffset() {
	// TODO
	//
	tool.FatalExit("Un-support operation")

	//opt.zkClient.Topic("").Partitions()[0]

	log.Printf("Reset zookeeper offset(%d) for topic(%s), group(%s), partition(%d) successfuly!",
		opt.resetOffset, opt.resetTopic, opt.resetGroupId, opt.resetPartition)
}

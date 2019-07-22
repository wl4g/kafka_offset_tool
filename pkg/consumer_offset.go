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
	"fmt"
	"github.com/Shopify/sarama"
	"kafka_offset_tool/pkg/common"
	"log"
	"sync"
)

type GroupConsumedOffsets map[string]map[string]map[int32]ConsumedOffset

type ConsumedOffset struct {
	ConsumedOffset int64
	Lag            int64
	Member         *sarama.GroupMemberDescription
	ConsumerType   string
	ProducedOffset
}

// Consumed offset toString
func (consumedOffset *ConsumedOffset) memberAsString() string {
	if consumedOffset.Member != nil {
		return fmt.Sprintf("%s%s", consumedOffset.Member.ClientId,
			consumedOffset.Member.ClientHost)
	}
	return None
}

const (
	ZKType = "ZK"
	KFType = "KF"
	None   = "None"
)

/**
 * Get consumer group member by topic and partition.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func getGroupMember(members map[string]*sarama.GroupMemberDescription,
	topic string, partition int32) *sarama.GroupMemberDescription {
	for _, member := range members {
		memberAssign, _ := member.GetMemberAssignment()
		for _topic, partitions := range memberAssign.Topics {
			if _topic == topic {
				for _, _partition := range partitions {
					if _partition == partition {
						return member
					}
				}
			}
		}
	}
	return nil // Not member.
}

/**
 * Analysis consumed topic partition offsets.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func analysisConsumedTopicPartitionOffsets(consumerType string) GroupConsumedOffsets {
	// Produced offsets of topics.
	producedOffsets := getProducedTopicPartitionOffsets()
	log.Printf("Extract & analysis group topic partition offset relation...")

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	// Consumed offsets of groups.
	consumedOffsets := make(GroupConsumedOffsets)

	// Group type filter.
	hasKfGroup, hasZkGroup := hasGroupType(consumerType)

	// --- Kafka direct consumed group offset. ---
	if hasKfGroup {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, broker := range listBrokers() {
				mu.Lock()
				groupIds := listKafkaGroupId(broker)
				mu.Unlock()

				// Describe groups.
				describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
				if err != nil {
					common.ErrorExit(err, "Cannot get describe groupId: %s, %s", groupIds)
				}
				for _, group := range describeGroups.Groups {
					consumedOffsets[group.GroupId] = make(map[string]map[int32]ConsumedOffset)

					// Group consumer by topic and partition all.
					offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
					for topic, partitions := range producedOffsets {
						for partition := range partitions {
							offsetFetchRequest.AddPartition(topic, partition)
						}
					}

					// Fetch offset all of group.
					offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
					if err != nil {
						common.ErrorExit(err, "Cannot get offset of group: %s, %s", group.GroupId)
					}
					for topic, partitions := range offsetFetchResponse.Blocks {
						consumedOffsets[group.GroupId][topic] = make(map[int32]ConsumedOffset)

						for partition, offsetFetchResponseBlock := range partitions {
							// for testing.
							//if "archiving_stream_test" == group.GroupId && "safeclound_air" == topic && partition == 7 {
							//	fmt.Printf("")
							//}

							err := offsetFetchResponseBlock.Err
							if err != sarama.ErrNoError {
								log.Printf("Error for partition: %d, %s", partition, err.Error())
								continue
							}

							// Current consumed offset.
							mu.Lock()
							_consumedOffset := ConsumedOffset{ConsumerType: KFType}
							_consumedOffset.ConsumedOffset = offsetFetchResponseBlock.Offset
							mu.Unlock()

							// Lag of group partition.
							if _producedOffset, e4 := producedOffsets[topic][partition]; e4 {
								// If the topic is consumed by that consumer group, but no offsetInfo associated with the partition
								// forcing lag to -1 to be able to alert on that
								var lag int64
								if offsetFetchResponseBlock.Offset == -1 {
									lag = -1
								} else {
									lag = _producedOffset.NewestOffset - offsetFetchResponseBlock.Offset
								}

								mu.Lock()
								_consumedOffset.Lag = lag
								_consumedOffset.NewestOffset = _producedOffset.NewestOffset
								_consumedOffset.OldestOffset = _producedOffset.OldestOffset
								mu.Unlock()
							} else {
								log.Printf("No offsetInfo of topic: %s, partition: %d, %v", topic, partition, e4)
							}

							// Consumed group member.
							mu.Lock()
							_consumedOffset.Member = getGroupMember(group.Members, topic, partition)
							consumedOffsets[group.GroupId][topic][partition] = _consumedOffset
							mu.Unlock()
						}
					}
				}
			}
		}()
	}

	// --- Zookeeper consumed group offset. ---
	if hasZkGroup {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if zkConsumerGroups, e5 := opt.zkClient.Consumergroups(); e5 != nil {
				log.Printf("Cannot get consumer group(zookeeper). %v", e5)
			} else {
				for _, zkGroup := range zkConsumerGroups {
					mu.Lock()
					consumedOffsets[zkGroup.Name] = make(map[string]map[int32]ConsumedOffset)
					mu.Unlock()

					topics, _ := zkGroup.Topics()
					for _, zkTopic := range topics {
						mu.Lock()
						consumedOffsets[zkGroup.Name][zkTopic.Name] = make(map[int32]ConsumedOffset)
						mu.Unlock()

						zkPartitions, _ := zkTopic.Partitions()
						for _, zkPartition := range zkPartitions {
							mu.Lock()
							_consumedOffset := ConsumedOffset{ConsumerType: ZKType}
							mu.Unlock()

							// Current consumed offset.
							zkConsumedOffset, _ := zkGroup.FetchOffset(zkTopic.Name, zkPartition.ID)
							mu.Lock()
							_consumedOffset.ConsumedOffset = zkConsumedOffset
							mu.Unlock()
							// Lag
							if zkConsumedOffset > 0 {
								mu.Lock()
								_consumedOffset.Lag = producedOffsets[zkTopic.Name][zkPartition.ID].NewestOffset - zkConsumedOffset
								mu.Unlock()
							}

							mu.Lock()
							consumedOffsets[zkGroup.Name][zkTopic.Name][zkPartition.ID] = _consumedOffset
							mu.Unlock()
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	return consumedOffsets
}

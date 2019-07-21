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
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/krallistic/kazoo-go"
	"kafka_offset_tool/pkg/tool"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type ProducedOffset struct {
	NewestOffset int64 // LogSize
	OldestOffset int64
}

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

// Connected to kafka & zk servers.
func ensureConnected() error {
	if opt.client == nil {
		log.Printf("Connect to kafka servers...")

		// Init configuration.
		config := sarama.NewConfig()
		config.ClientID = fmt.Sprintf("kafkaOffsetTool-%d", rand.Int())
		if kafkaVer, e1 := sarama.ParseKafkaVersion(opt.kafkaVersion); e1 == nil {
			config.Version = kafkaVer
		} else {
			tool.ErrorExit(e1, "Unrecognizable kafka version.")
		}

		// Connect kafka brokers.
		if client, e2 := sarama.NewClient(strings.Split(opt.brokers, ","), config); e2 == nil {
			opt.client = client
		} else {
			tool.ErrorExit(e2, "Unable connect kafka brokers. %s", opt.brokers)
		}
		// defer opt.client.Close()

		// Connect zookeeper servers.
		if zkClient, e3 := kazoo.NewKazoo(strings.Split(opt.zkServers, ","), nil); e3 == nil {
			opt.zkClient = zkClient
			// Do you have to get it to check the connection ???
			if _, e4 := zkClient.Brokers(); e4 != nil {
				tool.ErrorExit(e4, "Unable connect zk servers. %s", opt.zkServers)
			}
		} else {
			tool.ErrorExit(e3, "Unable connect zk servers. %s", opt.zkServers)
		}
		// defer opt.zkClient.Close()
	}
	return nil
}

// List of brokers on kafka direct.
func listBrokers() []*sarama.Broker {
	brokers := opt.client.Brokers()
	if len(brokers) <= 0 {
		tool.FatalExit("Cannot get brokers.")
	}
	return brokers
}

// List of groupId all on kafka broker direct.
func listKafkaGroupIdAll() []string {
	groupIdAll := make([]string, 0)
	for _, broker := range listBrokers() {
		for _, groupId := range listKafkaGroupId(broker) {
			groupIdAll = append(groupIdAll, groupId)
		}
	}
	return groupIdAll
}

// List of groupIds on kafka broker direct.
func listKafkaGroupId(broker *sarama.Broker) []string {
	if err := broker.Open(opt.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
		tool.ErrorExit(err, "Cannot connect to brokerID: %d, %s", broker.ID())
	}

	// Get groupIds.
	groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		tool.ErrorExit(err, "Cannot get kafka groups.")
	}
	groupIds := make([]string, 0)
	for groupId := range groups.Groups {
		groupIds = append(groupIds, groupId)
	}
	return groupIds
}

// List of topics on kafka broker direct.
func listTopicAll() []string {
	var topics, err = opt.client.Topics()
	if err != nil {
		tool.ErrorExit(err, "Cannot get topics.")
	}
	//log.Printf("Got size of topics: %d", len(topics))
	return topics
}

// Get consumer group member by topic and partition.
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

// Analysis consumed topic partition offsets.
func analysisConsumedTopicPartitionOffsets() map[string]map[string]map[int32]ConsumedOffset {
	// Produced offsets of topics.
	producedOffsets := getProducedTopicPartitionOffsets()
	log.Printf("Extract & analysis group topic partition offset relation...")

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	// Consumed offsets of groups.
	consumedOffsets := make(map[string]map[string]map[int32]ConsumedOffset)

	// --- Kafka direct consumed group offset. ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, broker := range listBrokers() {
			groupIds := listKafkaGroupId(broker)

			// Describe groups.
			describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
			if err != nil {
				tool.ErrorExit(err, "Cannot get describe groupId: %s, %s", groupIds)
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
					tool.ErrorExit(err, "Cannot get offset of group: %s, %s", group.GroupId)
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

	// --- Zookeeper consumed group offset. ---
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
	wg.Wait()

	return consumedOffsets
}

// Produced topic partition offsets.
func getProducedTopicPartitionOffsets() map[string]map[int32]ProducedOffset {
	log.Printf("Fetching metadata of the topic partitions infor...")

	// Describe topic partition offset.
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	producedTopicOffsets := make(map[string]map[int32]ProducedOffset)
	for _, topic := range listTopicAll() {
		//log.Printf("Fetching partition info for topics: %s ...", topic)

		go func(topic string) {
			wg.Add(1)
			defer wg.Done()
			partitions, err := opt.client.Partitions(topic)
			if err != nil {
				tool.ErrorExit(err, "Cannot get partitions of topic: %s, %s", topic)
			}
			mu.Lock()
			producedTopicOffsets[topic] = make(map[int32]ProducedOffset, len(partitions))
			mu.Unlock()

			for _, partition := range partitions {
				//fmt.Printf("topic:%s, part:%d \n", topic, partition)
				mu.Lock()
				_topicOffset := ProducedOffset{}
				mu.Unlock()

				// Largest offset(logSize).
				newestOffset, err := opt.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					tool.ErrorExit(err, "Cannot get current offset of topic: %s, partition: %d, %s", topic, partition)
				} else {
					mu.Lock()
					_topicOffset.NewestOffset = newestOffset
					mu.Unlock()
				}

				// Oldest offset.
				oldestOffset, err := opt.client.GetOffset(topic, partition, sarama.OffsetOldest)
				if err != nil {
					tool.ErrorExit(err, "Cannot get current oldest offset of topic: %s, partition: %d, %s", topic, partition)
				} else {
					mu.Lock()
					_topicOffset.OldestOffset = oldestOffset
					mu.Unlock()
				}

				mu.Lock()
				producedTopicOffsets[topic][partition] = _topicOffset
				mu.Unlock()
			}
		}(topic)
	}
	wg.Wait()
	return producedTopicOffsets
}

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
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(opt.resetGroupId, opt.client)
	var pom, _ = offsetManager.ManagePartition(opt.resetTopic, int32(opt.resetPartition))

	log.Printf("Resetting kafka direct consumer group(%s) topic(%s) partition(%d) offset(%d) ...",
		opt.resetGroupId, opt.resetTopic, opt.resetPartition, opt.resetOffset)
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

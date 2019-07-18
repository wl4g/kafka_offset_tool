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
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
)

type kafkaOpts struct {
	action       string
	brokers      string
	groupFilter  string
	topicFilter  string
	offsetRanges string
	kafkaVersion string
}

type PartitionOffset struct {
	CurrentOffset int64
	OldestOffset  int64
	Lag           int64
	LogSize       int64
}

var (
	opts = kafkaOpts{}
)

/**
 * Parse usage options.
 */
func init() {
	flag.StringVar(&opts.action, "action", "list-group",
		"Kafka offset tool action.(default: list-group, option: list-group|list-topic|reset-offset)")
	flag.StringVar(&opts.brokers, "brokers", "localhost:9092", "Kafka broker servers.(default: localhost:9092, e.g. host1:9092,host2:9092)")
	flag.StringVar(&opts.groupFilter, "group-filter", "*", "Kafka group regular expression filter.(default: *)")
	flag.StringVar(&opts.topicFilter, "topic-filter", "*", "Kafka topic regular expression filter.(default: *)")
	flag.StringVar(&opts.offsetRanges, "offset-ranges", "*", "Kafka topic regular expression filter.(default: *)")
	flag.StringVar(&opts.kafkaVersion, "kafka-version", "0.10.0.0", "Kafka support version.(default: 0.10.0.0, e.g. 0.9.0.0|1.0.0|1.1.1)")
	flag.Parse()

	// Printf usage, --help arg flag has built-in processed.
	if flag.NFlag() <= 0 {
		flag.Usage()
		os.Exit(0)
	}
	// log.Printf("--- kafka options ---\n%s", opts)
}

func main() {
	// Init connect.
	client := connectServers()

	// Extract topic partition offset of groups.
	var groupTopicPartOffsets = analysisGroupTopicPartitionOffsets(client)
	fmt.Print(groupTopicPartOffsets)
}

// Connect to kafka broker servers.
func connectServers() sarama.Client {
	log.Printf("Connect to kafka servers...")

	// Init configuration.
	config := sarama.NewConfig()
	config.ClientID = fmt.Sprintf("kafkaOffsetTool-%d", rand.Int())
	if kafkaVer, e1 := sarama.ParseKafkaVersion(opts.kafkaVersion); e1 == nil {
		config.Version = kafkaVer
	} else {
		log.Panicf("Unrecognizable kafka version. %s", e1)
	}

	// Create kafka client.
	var client, e2 = sarama.NewClient(strings.Split(opts.brokers, ","), config)
	if e2 != nil {
		log.Panicf("Unable connect kafka brokers. %s", e2)
	}
	// defer client.Close()
	return client
}

// Extract and analysis topic partition offsets of groups.
func analysisGroupTopicPartitionOffsets(client sarama.Client) map[string]map[string]map[int32]PartitionOffset {
	// Get partition offsets of topics.
	var topicPartOffsets = topicPartitionOffsets(client)
	log.Printf("Analysis topic partition offset relation basis on groups...")

	// Get group offsets info.
	groupTopicPartOffsets := make(map[string]map[string]map[int32]PartitionOffset)
	if len(client.Brokers()) <= 0 {
		log.Panicf("Cannot get topic group information, no valid broker.")
	}

	for _, broker := range client.Brokers() {
		if err := broker.Open(client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			log.Panicf("Cannot connect to brokerID: %d, %s", broker.ID(), err)
		}

		// Get groupIds.
		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			log.Panicf("Cannot get kafka groups. %s", err)
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			groupIds = append(groupIds, groupId)
		}

		// Get groups describe.
		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			log.Panicf("Cannot get describe groupId: %s, %s", groupIds, err)
		}
		// Get group offsets by topic and partition.
		for _, group := range describeGroups.Groups {
			groupTopicPartOffsets[group.GroupId] = make(map[string]map[int32]PartitionOffset)

			// Group consumer by topic and partition all.
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
			for topic, partitions := range topicPartOffsets {
				for partition := range partitions {
					offsetFetchRequest.AddPartition(topic, partition)
				}
			}

			// Fetch offset all of group.
			offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
			if err != nil {
				log.Panicf("Cannot get offset of group: %s, %s", group.GroupId, err)
			}
			for topic, partitions := range offsetFetchResponse.Blocks {
				groupTopicPartOffsets[group.GroupId][topic] = make(map[int32]PartitionOffset)
				for partition, offsetFetchResponseBlock := range partitions {
					err := offsetFetchResponseBlock.Err
					if err != sarama.ErrNoError {
						log.Printf("Error for partition: %d, %s", partition, err)
						continue
					}
					// Current offset of group partition .
					partitionOffset := PartitionOffset{}
					groupTopicPartOffsets[group.GroupId][topic][partition] = partitionOffset
					partitionOffset.CurrentOffset = offsetFetchResponseBlock.Offset

					// Lag of group partition.
					if _partitionOffset, ok := topicPartOffsets[topic][partition]; ok {
						// If the topic is consumed by that consumer group, but no offsetInfo associated with the partition
						// forcing lag to -1 to be able to alert on that
						var lag int64
						if offsetFetchResponseBlock.Offset == -1 {
							lag = -1
						} else {
							lag = _partitionOffset.CurrentOffset - offsetFetchResponseBlock.Offset
						}
						partitionOffset.Lag = lag
					} else {
						log.Printf("No offsetInfo of topic: %s, partition: %d, %s", topic, partition, err)
					}
				}
			}
		}
	}
	return groupTopicPartOffsets
}

// Get partitions offsets of topics.
func topicPartitionOffsets(client sarama.Client) map[string]map[int32]PartitionOffset {
	log.Printf("Fetching metadata of the topic partitions infor...")

	var topics, err = client.Topics()
	if err != nil {
		log.Panicf("Cannot get topics. %s", err)
	}
	//log.Printf("Got size of topics: %d", len(topics))

	// Describe topic partition offset.
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	topicPartOffsets := make(map[string]map[int32]PartitionOffset)
	for _, topic := range topics {
		//log.Printf("Fetching partition info for topics: %s ...", topic)

		go func(topic string) {
			wg.Add(1)
			defer wg.Done()
			partitions, err := client.Partitions(topic)
			if err != nil {
				log.Panicf("Cannot get partitions of topic: %s, %s", topic, err)
			}
			mu.Lock()
			topicPartOffsets[topic] = make(map[int32]PartitionOffset, len(partitions))
			mu.Unlock()

			for _, partition := range partitions {
				//fmt.Printf("topic:%s, part:%d \n", topic, partition)
				mu.Lock()
				partitionOffset := PartitionOffset{}
				topicPartOffsets[topic][partition] = partitionOffset
				mu.Unlock()

				// Current offset.
				currentOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Panicf("Cannot get current offset of topic: %s, partition: %d, %s", topic, partition, err)
				} else {
					mu.Lock()
					partitionOffset.CurrentOffset = currentOffset
					mu.Unlock()
				}

				// Oldest offset.
				oldestOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
				if err != nil {
					log.Panicf("Cannot get current oldest offset of topic: %s, partition: %d, %s", topic, partition, err)
				} else {
					mu.Lock()
					partitionOffset.OldestOffset = oldestOffset
					mu.Unlock()
				}
			}
		}(topic)
	}
	wg.Wait()
	return topicPartOffsets
}

// Reset topic group partitions offset.
// See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
func resetOffset(client sarama.Client, offset int64, topic string, groupId string, partition int32) {
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(groupId, client)
	fmt.Print(offsetManager)
	var pom, _ = offsetManager.ManagePartition(topic, partition)
	pom.ResetOffset(offset, "modified_meta")

	log.Printf("Reseted offset: %d, topic: %s, group: %s, partition: %d", offset, topic, groupId, partition)
}

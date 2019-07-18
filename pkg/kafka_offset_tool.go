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
	"github.com/urfave/cli"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
)

type kafkaOption struct {
	client sarama.Client

	brokers      string
	kafkaVersion string

	groupFilter    string
	topicFilter    string
	consumerFilter string

	resetGroupId   string
	resetTopic     string
	resetPartition int
	resetOffset    int64
}

type PartitionOffset struct {
	CurrentOffset int64
	OldestOffset  int64
	Lag           int64
	LogSize       int64
}

var (
	opt = kafkaOption{}
)

/**
 * Parse usage options.</br>
 * See: https://github.com/urfave/cli#examples
 */
func main() {
	app := cli.NewApp()
	app.Version = "v1.0.0"
	app.Authors = []cli.Author{
		{Name: "Wag sir", Email: "983708408@qq.com"},
	}
	app.Description = "KafkaOffsetTool is a lightweight tool for Kafka offset operation and maintenance."
	app.Copyright = "(c) 1999 Serious Enterprise"
	app.Commands = cli.Commands{
		{
			Name:        "list-group",
			Usage:       "list-group [OPTION]...",
			Description: "Get the group list.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,s", Value: "localhost:9092", Usage: "(default: localhost:9092) --brokers=127.0.0.1:9092",
					Destination: &opt.brokers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "(default: 0.10.0.0) --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "filter,f", Value: "*", Usage: "(default: *) --filter=myPrefix\\\\S*"},
			},
			Before: func(c *cli.Context) error {
				return clientConnect()
			},
			Action: func(c *cli.Context) error {
				//fmt.Fprintf(c.App.Writer, ":list-group--processing, %s", c.String("filter"))
				return nil
			},
		},
		{
			Name:        "list-topic",
			Usage:       "list-topic [OPTION]...",
			Description: "Get the topic list.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,s", Value: "localhost:9092", Usage: "(default: localhost:9092) --brokers=127.0.0.1:9092",
					Destination: &opt.brokers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "(default: 0.10.0.0) --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "filter,f", Value: "*", Usage: "(default: *) --filter=myPrefix\\\\S*"},
			},
			Before: func(c *cli.Context) error {
				return clientConnect()
			},
			Action: func(c *cli.Context) error {
				return nil
			},
		},
		{
			Name:        "list-consumer",
			Usage:       "list-consumer [OPTION]...",
			Description: "Get the consumer list.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,s", Value: "localhost:9092", Usage: "(default: localhost:9092) --brokers=127.0.0.1:9092",
					Destination: &opt.brokers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "(default: 0.10.0.0) --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "filter,f", Value: "*", Usage: "(default: *) --filter=myPrefix\\\\S*"},
				cli.StringFlag{Name: "type,t", Value: "*", Usage: "(default: *) --type=zk|kf"},
			},
			Before: func(c *cli.Context) error {
				return clientConnect()
			},
			Action: func(c *cli.Context) error {
				return nil
			},
		},
		{
			Name:        "reset-offset",
			Usage:       "reset-offset [OPTION]...",
			Description: "Reset the offset of the specified grouping topic partition.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,s", Value: "localhost:9092", Usage: "(default: localhost:9092) --brokers=127.0.0.1:9092",
					Destination: &opt.brokers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "(default: 0.10.0.0) --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "group,g", Usage: "--group=myGroup", Destination: &opt.resetGroupId},
				cli.StringFlag{Name: "topic,t", Usage: "--topic=myTopic", Destination: &opt.resetTopic},
				cli.IntFlag{Name: "partition,p", Usage: "--partition=0", Destination: &opt.resetPartition},
				cli.Int64Flag{Name: "offset,f", Usage: "--partition=0", Destination: &opt.resetOffset},
			},
			Before: func(c *cli.Context) error {
				return clientConnect()
			},
			Action: func(c *cli.Context) error {
				resetOffset()
				return nil
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Printf("See 'kafkaOffsetTool --help'. %s", err.Error())
	}
}

// Connect to kafka broker servers.
func clientConnect() error {
	if opt.client == nil {
		log.Printf("Connect to kafka servers...")

		// Init configuration.
		config := sarama.NewConfig()
		config.ClientID = fmt.Sprintf("kafkaOffsetTool-%d", rand.Int())
		if kafkaVer, e1 := sarama.ParseKafkaVersion(opt.kafkaVersion); e1 == nil {
			config.Version = kafkaVer
		} else {
			log.Panicf("Unrecognizable kafka version. %s", e1)
		}

		// Create kafka client.
		var client, e2 = sarama.NewClient(strings.Split(opt.brokers, ","), config)
		if e2 != nil {
			log.Panicf("Unable connect kafka brokers. %s", e2)
		}
		// defer client.Close()
		opt.client = client
		return e2
	}
	return nil
}

// Analysis and extract topic partition offsets of groups.
func analysisGroupTopicPartitionOffsets() map[string]map[string]map[int32]PartitionOffset {
	// Get partition offsets of topics.
	var topicPartOffsets = topicPartitionOffsets()
	log.Printf("Analysis topic partition offset relation basis on groups...")

	// Get group offsets info.
	groupTopicPartOffsets := make(map[string]map[string]map[int32]PartitionOffset)
	if len(opt.client.Brokers()) <= 0 {
		log.Panicf("Cannot get topic group information, no valid broker.")
	}

	for _, broker := range opt.client.Brokers() {
		if err := broker.Open(opt.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
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
func topicPartitionOffsets() map[string]map[int32]PartitionOffset {
	log.Printf("Fetching metadata of the topic partitions infor...")

	var topics, err = opt.client.Topics()
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
			partitions, err := opt.client.Partitions(topic)
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
				currentOffset, err := opt.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Panicf("Cannot get current offset of topic: %s, partition: %d, %s", topic, partition, err)
				} else {
					mu.Lock()
					partitionOffset.CurrentOffset = currentOffset
					mu.Unlock()
				}

				// Oldest offset.
				oldestOffset, err := opt.client.GetOffset(topic, partition, sarama.OffsetOldest)
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
func resetOffset() {
	var offsetManager, _ = sarama.NewOffsetManagerFromClient(opt.resetGroupId, opt.client)
	fmt.Print(offsetManager)
	var pom, _ = offsetManager.ManagePartition(opt.resetTopic, int32(opt.resetPartition))
	pom.ResetOffset(opt.resetOffset, "modified_meta")

	log.Printf("Reseted offset: %d, topic: %s, group: %s, partition: %d",
		opt.resetOffset, opt.resetTopic, opt.resetGroupId, opt.resetPartition)
}

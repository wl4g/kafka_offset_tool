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

var (
	opts          = kafkaOpts{}
	wg            sync.WaitGroup
	kafkaConsumer sarama.Consumer
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

	log.Printf("--- kafka options ---\n%s", opts)
}

// Using kafka client, See: https://github.com/Shopify/sarama/blob/master/offset_manager_test.go#L228
func main() {
	log.Printf("Initial kafka connect ...")

	// Init configuration.
	config := sarama.NewConfig()
	config.ClientID = fmt.Sprintf("kafkaOffsetTool-%d", rand.Int())
	var kafkaVer, e1 = sarama.ParseKafkaVersion(opts.kafkaVersion)
	if e1 != nil {
		panic(e1)
	}
	config.Version = kafkaVer

	// Create kafka client.
	var client, e2 = sarama.NewClient(strings.Split(opts.brokers, ","), config)
	if e2 != nil {
		panic(e2)
	}
	var offsetManager, _ = sarama.NewOffsetManagerFromClient("", client)
	fmt.Print(offsetManager)

	// ------- Kafka topic partition offset info. ------------

	// ------- Kafka group offset info. ----------------------
	if len(client.Brokers()) > 0 {
		for _, broker := range client.Brokers() {
			if err := broker.Open(client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
				log.Printf("Cannot connect to brokerID: %d, %s", broker.ID(), err)
				panic(err)
			}
			// Get groupIds.
			groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
			if err != nil {
				log.Printf("Cannot get kafka groups. %s", err)
				panic(err)
			}
			groupIds := make([]string, 0)
			for groupId := range groups.Groups {
				groupIds = append(groupIds, groupId)
			}
			// Get groups describe.
			describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
			if err != nil {
				log.Printf("Cannot get describe groups. %s", err)
				panic(err)
			}
			// Get groups offset info.
			for _, group := range describeGroups.Groups {
				offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
				for topic, partitions := range offset {
					for partition := range partitions {
						offsetFetchRequest.AddPartition(topic, partition)
					}
				}
			}
		}
	} else {
		panic("No valid broker, cannot get consumer group metrics")
	}
	log.Printf("Kafka operation finished!")
}

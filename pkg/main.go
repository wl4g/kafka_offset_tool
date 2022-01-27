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
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"kafka_offset_tool/pkg/common"

	"github.com/Shopify/sarama"
	"github.com/krallistic/kazoo-go"
	"github.com/urfave/cli"
)

const (
	BANNER = ` 
_  __      __ _            ____   __  __          _    _______          _ 
| |/ /     / _| |          / __ \ / _|/ _|        | | |__   __|        | |
| ' / __ _| |_| | ____ _  | |  | | |_| |_ ___  ___| |_   | | ___   ___ | |
|  < / _' |  _| |/ / _' | | |  | |  _|  _/ __|/ _ \ __|  | |/ _ \ / _ \| |
| . \ (_| | | |   < (_| | | |__| | | | | \__ \  __/ |_   | | (_) | (_) | |
|_|\_\__,_|_| |_|\_\__,_|  \____/|_| |_| |___/\___|\__|  |_|\___/ \___/|_|
	`
	DESCRIPTION = "KafkaOffsetTool is a lightweight common for Kafka offset operation and maintenance."
	VERSION     = "v1.2.6"
	WIKI        = "https://github.com/wl4g/kafka_offset_tool/blob/master/README.md"
	AUTHORS     = "Wanglsir@gmail.com, 983708408@qq.com"
)

type kafkaOption struct {
	client   sarama.Client
	zkClient *kazoo.Kazoo

	brokers      string
	kafkaVersion string
	zkServers    string

	groupFilter    string
	topicFilter    string
	consumerFilter string
	consumerType   string

	outputFile string // Ouput file path.
	inputFile  string // Input file path.

	setGroupId   string
	setTopic     string
	setPartition int64
	setOffset    int64

	increment string
}

var (
	option = kafkaOption{}
)

func main() {
	runCommand()
}

/**
 * Parse usage options.</br>
 * See: https://github.com/urfave/cli#examples
 * @author Wang.sir <wanglsir@gmail.com, 983708408@qq.com>
 * @date 19-07-20
 */
func runCommand() {
	fmt.Printf("%s\n", BANNER)
	fmt.Printf("VERSION: %s\n", VERSION)
	fmt.Printf("AUTHORS: %s\n", AUTHORS)
	fmt.Printf("WIKI: %s\n", WIKI)
	fmt.Printf("TIME: %s\n\n", time.Now().Format(time.RFC3339))

	app := cli.NewApp()
	app.HideVersion = true
	app.Commands = cli.Commands{
		{
			Name:        "get-group",
			Usage:       "get-group [OPTIONS]...",
			Description: "Gets the kafka consumer groups information.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Value: "127.0.0.1:9092", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &option.brokers},
				cli.StringFlag{Name: "zkServers,z", Value: "127.0.0.1:2181", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &option.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "Sarama client connect backward compatible version of Kafka (major.minor.veryMinor.patch). default: 0.10.0.0",
					Destination: &option.kafkaVersion},
				cli.StringFlag{Name: "filter,f", Value: "*", Usage: "GroupId regex filter. e.g. --filter='(^console\\S+)'",
					Destination: &option.groupFilter},
				cli.StringFlag{Name: "type,t", Value: "*", Usage: "e.g. --type=zk|kf|*", Destination: &option.consumerType},
			},
			Before: func(c *cli.Context) error {
				if common.IsAnyBlank(option.brokers, option.zkServers) {
					common.FatalExit("Arguments brokers,zkServers is required")
				}
				if !common.StringsContains([]string{ZKType, KFType, "*"}, option.consumerType, true) {
					common.FatalExit("Failed to get list of groups, un-support consumer type %s",
						option.consumerType)
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				begin := time.Now().UnixNano()
				dataset := make([][]interface{}, 0)
				for groupIdName, _consumerType := range listGroupIdAll() {
					// New print row.
					if common.Match(option.groupFilter, groupIdName) {
						row := []interface{}{groupIdName, _consumerType}
						dataset = append(dataset, row)
					}
				}
				// Grid print.
				common.GridPrinf("Consumer group information", []string{"Group", "Type"}, dataset)

				// Cost statistics.
				log.Printf(" => Result: %d row processed (%f second) finished!", len(dataset),
					common.CostSecond(begin))
				return nil
			},
		},
		{
			Name:        "get-topic",
			Usage:       "get-topic [OPTIONS]...",
			Description: "Gets the kafka topics information.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Value: "127.0.0.1:9092", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &option.brokers},
				cli.StringFlag{Name: "zkServers,z", Value: "127.0.0.1:2181", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &option.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "Sarama client connect backward compatible version of Kafka (major.minor.veryMinor.patch). default: 0.10.0.0",
					Destination: &option.kafkaVersion},
				cli.StringFlag{Name: "filter,f", Value: "*", Usage: "Topic regex filter. e.g. --filter='(^console\\S+)'", Destination: &option.topicFilter},
			},
			Before: func(c *cli.Context) error {
				if common.IsAnyBlank(option.brokers, option.zkServers) {
					common.FatalExit("Arguments brokers,zkServers is required")
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				begin := time.Now().UnixNano()
				dataset := make([]string, 0)
				for _, topicName := range listTopicAll() {
					// New print row.
					if common.Match(option.topicFilter, topicName) {
						dataset = append(dataset, topicName)
					}
				}
				common.SimplePrinf("List of topics information.", dataset)
				log.Printf(" => Result: %d row processed (%f second) finished!", len(dataset),
					common.CostSecond(begin))
				return nil
			},
		},
		{
			Name:        "get-offset",
			Usage:       "get-offset [OPTIONS]...",
			Description: "Gets the kafka consumer group offsets information.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Value: "127.0.0.1:9092", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &option.brokers},
				cli.StringFlag{Name: "zkServers,z", Value: "127.0.0.1:2181", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &option.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "Sarama client connect backward compatible version of Kafka (major.minor.veryMinor.patch). default: 0.10.0.0",
					Destination: &option.kafkaVersion},
				cli.StringFlag{Name: "groupFilter", Value: "*", Usage: "GroupId regex filter. e.g. --groupFilter='(^console\\S+)'",
					Destination: &option.groupFilter},
				cli.StringFlag{Name: "topicFilter", Value: "*", Usage: "Topic regex filter. e.g. --topicFilter='(^console\\S+)'",
					Destination: &option.topicFilter},
				cli.StringFlag{Name: "consumerFilter", Value: "*", Usage: "Consumer regex filter. e.g. --consumerFilter='(^console\\S+)'",
					Destination: &option.consumerFilter},
				cli.StringFlag{Name: "type,t", Value: "*", Usage: "e.g. --type=zk|kf|*",
					Destination: &option.consumerType},
				cli.StringFlag{Name: "outputFile,o", Usage: "e.g. --outputFile=myoffset.json", Destination: &option.outputFile},
			},
			Before: func(c *cli.Context) error {
				if common.IsAnyBlank(option.brokers, option.zkServers) {
					common.FatalExit("Arguments brokers,zkServers is required")
				}
				if !(common.StringsContains([]string{ZKType, KFType, "*"}, option.consumerType, true)) {
					common.FatalExit("Invalid consumer type. %s", option.consumerType)
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				begin := time.Now().UnixNano()
				// Extract & analysis consumed partition offsets.
				fetchedConsumedOffset := fetchConsumedTopicPartitionOffsets(option.consumerType)

				// Filtering by group.
				for group, consumedTopicOffset := range fetchedConsumedOffset {
					if !common.Match(option.groupFilter, group) {
						delete(fetchedConsumedOffset, group)
					} else {
						// Filtering by topic.
						for topic, partitionOffset := range consumedTopicOffset {
							if !common.Match(option.topicFilter, topic) {
								delete(consumedTopicOffset, topic)
							} else {
								// Filtering by consumer.
								for partition, consumedOffset := range partitionOffset {
									memberString := consumedOffset.memberAsString()
									if !common.Match(option.consumerFilter, memberString) {
										delete(partitionOffset, partition)
									}
								}
							}
						}
					}
				}

				// export?
				if !common.IsBlank(option.outputFile) {
					data := []byte(common.ToJSONString(fetchedConsumedOffset, true))
					if err := common.WriteFile(option.outputFile, data, false); err != nil {
						common.ErrorExit(err, "Failed to export consumed offset to '%s'", option.outputFile)
					}
					// Cost statistics.
					log.Printf(" => Exported to %s (%f second) finished!", option.outputFile, common.CostSecond(begin))
				} else { // Grid print.
					// Transform to dataset
					dataset := make([][]interface{}, 0)
					for group, consumedTopicOffset := range fetchedConsumedOffset {
						for topic, partitionOffset := range consumedTopicOffset {
							for partition, consumedOffset := range partitionOffset {
								memberString := consumedOffset.memberAsString()
								// New print row.
								row := []interface{}{group, topic,
									strconv.FormatInt(int64(partition), 10),
									strconv.FormatInt(consumedOffset.OldestOffset, 10),
									strconv.FormatInt(consumedOffset.NewestOffset, 10),
									strconv.FormatInt(consumedOffset.Lag, 10),
									strconv.FormatInt(consumedOffset.ConsumedOffset, 10),
									memberString, consumedOffset.ConsumerType}
								dataset = append(dataset, row)
							}
						}
					}
					common.GridPrinf("Consumer grouping describe list", []string{"Group", "Topic", "Partition",
						"OldestOffset", "NewestOffset", "Lag", "ConsumedOffset", "ConsumerOwner", "Type"}, dataset)
					// Cost statistics.
					log.Printf(" => Result: %d row processed (%f second) finished!", len(dataset),
						common.CostSecond(begin))
				}
				return nil
			},
		},
		{
			Name:        "set-offset",
			Usage:       "set-offset [OPTIONS]...",
			Description: "set the offset of the specified kafka group topic partition.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Value: "127.0.0.1:9092", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &option.brokers},
				cli.StringFlag{Name: "zkServers,z", Value: "127.0.0.1:2181", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &option.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "Sarama client connect backward compatible version of Kafka (major.minor.veryMinor.patch). default: 0.10.0.0",
					Destination: &option.kafkaVersion},
				cli.StringFlag{Name: "group,g", Usage: "Specifies which consumer group offset to set. e.g. --group=mygroup", Destination: &option.setGroupId},
				cli.StringFlag{Name: "topic,t", Usage: "Specifies which topic offset to set. e.g. --topic=mytopic", Destination: &option.setTopic},
				cli.Int64Flag{Name: "partition,p", Usage: "Specifies which partition offset to set. e.g. --partition=0", Destination: &option.setPartition},
				cli.Int64Flag{Name: "offset,f", Usage: "Specifies the offset value to set (>= 0). e.g. --offset=0", Destination: &option.setOffset},
				cli.StringFlag{Name: "inputFile,i", Usage: "Load the offset configuration to set from the local file path, If it exists simultaneously with the arg 'group/topic/partition/offset', only this arg takes effect. e.g. --inputFile=myoffset.json", Destination: &option.inputFile},
			},
			Before: func(c *cli.Context) error {
				if common.IsAnyBlank(option.brokers, option.zkServers) {
					common.FatalExit("Invalid arguments brokers,zkServers is required!")
				}
				if common.IsBlank(option.inputFile) {
					if common.IsAnyBlank(option.setGroupId, option.setTopic) || option.setPartition == 0 || option.setOffset == 0 {
						common.FatalExit("Invalid arguments '--topic,-t and --partition,-p and --offset,-f is required, and partition,offset(must >=0)")
					}
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				setOffset()
				return nil
			},
		},
		{
			Name:        "calc-offset",
			Usage:       "calc-offset [OPTIONS]...",
			Description: "Tool commands for calculating kafka offsets in configuration.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "inputFile,i", Required: true, Usage: "Load the offset configuration to set from the local file path. e.g. -i=myoffset1.json", Destination: &option.inputFile},
				cli.StringFlag{Name: "outputFile,o", Required: true, Usage: "Output the calculated configuration to the local file. e.g. -o=myoffset2.json", Destination: &option.outputFile},
				cli.StringFlag{Name: "increment,I", Required: true, Usage: "The increment logSize percentage used to calculate the offset(Negative are allowed). Equivalent to: newConsumerdOffset=oldOffset+incrPercent*(newOffset-oldOffset). for example: -I=-0.1", Destination: &option.increment},
			},
			Before: func(c *cli.Context) error {
				// if common.IsAnyBlank(option.inputFile, option.outputFile) || option.increment == "" {
				// 	common.FatalExit("Invalid arguments '--inputFile,-i/--outputFile,-o/--increment,-I' is required")
				// }
				return nil
			},
			Action: func(c *cli.Context) error {
				inputOffsets := make(GroupConsumedOffsets)
				common.ParseJSONFromFile(option.inputFile, &inputOffsets)

				// calculation offset
				incr, err := strconv.ParseFloat(option.increment, 10)
				if err != nil {
					common.ErrorExit(err, "Failed to calculation offsets.")
				}
				groupNamesOrdered, err1 := common.ToOrderedKeys(inputOffsets)
				if err1 != nil {
					common.FatalExit(err1.Error())
				}
				for _, group := range groupNamesOrdered {
					topicNamesOrdered, err2 := common.ToOrderedKeys(inputOffsets[group])
					if err2 != nil {
						common.FatalExit(err2.Error())
					}
					for _, topic := range topicNamesOrdered {
						partitions := inputOffsets[group][topic]
						partitionNumsOrdered, err3 := common.ToOrderedKeysInt(partitions)
						if err3 != nil {
							common.FatalExit(err3.Error())
						}
						for _, partition := range partitionNumsOrdered {
							consumedOffset := partitions[int32(partition)]
							if !(consumedOffset.ConsumedOffset >= consumedOffset.OldestOffset && consumedOffset.ConsumedOffset <= consumedOffset.NewestOffset && consumedOffset.OldestOffset >= -1) {
								common.Warning("Unable calculate offsets of %s/%s/%v, consumed: %v, old: %v, new: %v",
									group, topic, partition, consumedOffset.ConsumedOffset, consumedOffset.OldestOffset, consumedOffset.NewestOffset)
								continue
							}
							beforeChanged := consumedOffset.ConsumedOffset
							logSize := math.Abs(float64(consumedOffset.NewestOffset - consumedOffset.OldestOffset))
							afterChanged := consumedOffset.OldestOffset + int64(incr*logSize)
							if afterChanged < consumedOffset.OldestOffset {
								afterChanged = consumedOffset.OldestOffset
								common.Warning("%s/%s/%v, because offset(%v) < oldestOffset(%v) and use oldest offset",
									group, topic, partition, afterChanged, consumedOffset.OldestOffset)
							} else if afterChanged > consumedOffset.NewestOffset {
								afterChanged = consumedOffset.NewestOffset
								common.Warning("%s/%s/%v, because offset(%v) > newestOffset(%v) and use newest offset",
									group, topic, partition, afterChanged, consumedOffset.NewestOffset)
							}
							consumedOffset.ConsumedOffset = afterChanged
							consumedOffset.Lag = int64(math.Abs(float64(consumedOffset.NewestOffset - afterChanged)))
							partitions[int32(partition)] = consumedOffset
							log.Printf("Set to %s/%s/%v, offset: %v => %v",
								group, topic, partition, beforeChanged, consumedOffset.ConsumedOffset)
						}
					}
				}

				// Output new consumerd offsets file
				data := []byte(common.ToJSONString(inputOffsets, true))
				if err := common.WriteFile(option.outputFile, data, false); err != nil {
					common.ErrorExit(err, "Failed to export consumed offset to '%s'", option.outputFile)
				}
				return nil
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Printf("See 'kafkaOffsetTool --help'. %s", err.Error())
	}
}

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
	"github.com/krallistic/kazoo-go"
	"github.com/urfave/cli"
	"kafka_offset_tool/pkg/tool"
	"log"
	"os"
	"strconv"
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

	resetGroupId   string
	resetTopic     string
	resetPartition int
	resetOffset    int64
}

var (
	opt = kafkaOption{}
)

func main() {
	parseExecution()
}

/**
 * Parse usage options.</br>
 * See: https://github.com/urfave/cli#examples
 */
func parseExecution() {
	app := cli.NewApp()
	app.Name = "KafkaOffsetTool"
	app.Version = "v1.0.0"
	app.Authors = []cli.Author{
		{Name: "Wangl sir", Email: "983708408@qq.com"},
	}
	app.Description = "KafkaOffsetTool is a lightweight tool for Kafka offset operation and maintenance."
	app.Copyright = "(c) 1999 Serious Enterprise"
	app.Commands = cli.Commands{
		{
			Name:        "list-group",
			Usage:       "list-group [OPTION]...",
			Description: "Get the group list.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &opt.brokers},
				cli.StringFlag{Name: "zkServers,z", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &opt.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "e.g. (default: 0.10.0.0) --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "groupFilter,f", Value: "*", Usage: "e.g. --groupFilter=myPrefix\\\\S*"},
			},
			Before: func(c *cli.Context) error {
				if tool.IsAnyBlank(opt.brokers, opt.zkServers) {
					tool.FatalExit("Arguments brokers,zkServers is required")
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				//fmt.Fprintf(c.App.Writer, ":list-group--processing, %s", c.String("filter"))
				tool.PrintResult("List of groups information.", listKafkaGroupIdAll())
				return nil
			},
		},
		{
			Name:        "list-topic",
			Usage:       "list-topic [OPTION]...",
			Description: "Get the topic list.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &opt.brokers},
				cli.StringFlag{Name: "zkServers,z", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &opt.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "e.g. (default: 0.10.0.0) --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "filter,f", Value: "*", Usage: "e.g. --filter=myPrefix\\\\S*"},
			},
			Before: func(c *cli.Context) error {
				if tool.IsAnyBlank(opt.brokers, opt.zkServers) {
					tool.FatalExit("Arguments brokers,zkServers is required")
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				tool.PrintResult("List of topics information.", listKafkaTopicAll())
				return nil
			},
		},
		{
			Name:        "list-consumer",
			Usage:       "list-consumer [OPTION]...",
			Description: "Get the consumer list.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &opt.brokers},
				cli.StringFlag{Name: "zkServers,z", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &opt.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "e.g.  --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "groupFilter", Value: "*", Usage: "e.g. --groupFilter=myPrefix\\\\S*",
					Destination: &opt.groupFilter},
				cli.StringFlag{Name: "topicFilter", Value: "*", Usage: "e.g. --topicFilter=myPrefix\\\\S*",
					Destination: &opt.topicFilter},
				cli.StringFlag{Name: "consumerFilter", Value: "*", Usage: "e.g. --consumerFilter=myPrefix\\\\S*",
					Destination: &opt.consumerFilter},
				cli.StringFlag{Name: "type,t", Value: "*", Usage: "e.g. --type=zk|kf",
					Destination: &opt.consumerType},
			},
			Before: func(c *cli.Context) error {
				if tool.IsAnyBlank(opt.brokers, opt.zkServers) {
					tool.FatalExit("Arguments brokers,zkServers is required")
				}
				if !(tool.StringsContains([]string{ZKType, KFType, "*"}, opt.consumerType)) {
					tool.FatalExit("Invalid consumer type. %s", opt.consumerType)
				}
				return ensureConnected()
			},
			Action: func(c *cli.Context) error {
				dataset := make([][]interface{}, 0)
				// Extract & analysis consumed partition offsets.
				consumedOffset := analysisConsumedTopicPartitionOffsets()
				for group, consumedTopicOffset := range consumedOffset {
					if tool.Match(opt.groupFilter, group) {
						for topic, partitionOffset := range consumedTopicOffset {
							if tool.Match(opt.topicFilter, topic) {
								for partition, consumedOffset := range partitionOffset {
									memberString := consumedOffset.memberAsString()
									if tool.Match(opt.consumerFilter, memberString) {
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
						}
					}
				}
				tool.GridPrinf(dataset)
				log.Printf("Processed kafka offset finished!")
				return nil
			},
		},
		{
			Name:        "reset-offset",
			Usage:       "reset-offset [OPTION]...",
			Description: "Reset the offset of the specified grouping topic partition.",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "brokers,b", Usage: "e.g. --brokers=127.0.0.1:9092", Destination: &opt.brokers},
				cli.StringFlag{Name: "zkServers,z", Usage: "e.g. --zkServers=127.0.0.1:2181", Destination: &opt.zkServers},
				cli.StringFlag{Name: "version,v", Value: "0.10.0.0", Usage: "e.g. --version=0.10.0.0",
					Destination: &opt.kafkaVersion},
				cli.StringFlag{Name: "group,g", Usage: "e.g. --group=myGroup", Destination: &opt.resetGroupId},
				cli.StringFlag{Name: "topic,t", Usage: "e.g. --topic=myTopic", Destination: &opt.resetTopic},
				cli.IntFlag{Name: "partition,p", Usage: "e.g. --partition=0", Destination: &opt.resetPartition},
				cli.Int64Flag{Name: "offset,f", Usage: "e.g. --partition=0", Destination: &opt.resetOffset},
			},
			Before: func(c *cli.Context) error {
				if tool.IsAnyBlank(opt.brokers, opt.zkServers) {
					tool.FatalExit("Arguments brokers,zkServers is required")
				}
				return ensureConnected()
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

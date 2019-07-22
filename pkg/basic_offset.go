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
	"kafka_offset_tool/pkg/common"
	"log"
	"math/rand"
	"strings"
	"sync"
)

/**
 * Connected to kafka & zk servers.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-18
 */
func ensureConnected() error {
	if opt.client == nil {
		log.Printf("Connect to kafka servers...")

		// Init configuration.
		config := sarama.NewConfig()
		config.ClientID = fmt.Sprintf("kafkaOffsetTool-%d", rand.Int())
		if kafkaVer, e1 := sarama.ParseKafkaVersion(opt.kafkaVersion); e1 == nil {
			config.Version = kafkaVer
		} else {
			common.ErrorExit(e1, "Unrecognizable kafka version.")
		}

		// Connect kafka brokers.
		if client, e2 := sarama.NewClient(strings.Split(opt.brokers, ","), config); e2 == nil {
			opt.client = client
		} else {
			common.ErrorExit(e2, "Unable connect kafka brokers. %s", opt.brokers)
		}
		// defer opt.client.Close()

		// Connect zookeeper servers.
		if zkClient, e3 := kazoo.NewKazoo(strings.Split(opt.zkServers, ","), nil); e3 == nil {
			opt.zkClient = zkClient
			// Do you have to get it to check the connection ???
			if _, e4 := zkClient.Brokers(); e4 != nil {
				common.ErrorExit(e4, "Unable connect zk servers. %s", opt.zkServers)
			}
		} else {
			common.ErrorExit(e3, "Unable connect zk servers. %s", opt.zkServers)
		}
		// defer opt.zkClient.Close()
	}
	return nil
}

/**
 * List of brokers on kafka direct.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-18
 */
func listBrokers() []*sarama.Broker {
	brokers := opt.client.Brokers()
	if len(brokers) <= 0 {
		common.FatalExit("Cannot get brokers.")
	}
	return brokers
}

/**
 * List of (kafka direct and zk)groupId all on kafka broker direct.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-18
 */
func listGroupIdAll() map[string]string {
	var (
		groupIdAll = make(map[string]string, 0)
		hasKfGroup = false
		hasZkGroup = false
	)

	if strings.EqualFold(opt.consumerType, KFType) {
		hasKfGroup = true
	} else if strings.EqualFold(opt.consumerType, ZKType) {
		hasZkGroup = true
	} else if strings.EqualFold(opt.consumerType, "*") {
		hasKfGroup = true
		hasZkGroup = true
	} else {
		common.FatalExit("Failed to get list of groups, un-support consumer type %s", opt.consumerType)
	}

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	// Kafka direct groups.
	if hasKfGroup {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, kfGroupId := range listKafkaGroupIdAll() {
				mu.Lock()
				groupIdAll[kfGroupId] = KFType
				mu.Unlock()
			}
		}()
	}

	// Zookeeper groups.
	if hasZkGroup {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, zkGroupId := range listZkGroupIdAll() {
				groupIdAll[zkGroupId] = ZKType
			}
		}()
	}

	wg.Wait()
	return groupIdAll
}

/**
 * List of zookeeper groupId all on kafka broker direct.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-18
 */
func listZkGroupIdAll() []string {
	groupIds := make([]string, 0)
	if _consumerGroups, e1 := opt.zkClient.Consumergroups(); e1 != nil {
		common.ErrorExit(e1, "Failed to get consumer group by zk!")
	} else {
		for _, _consumerGroup := range _consumerGroups {
			groupIds = append(groupIds, _consumerGroup.Name)
		}
	}
	return groupIds
}

/**
 * List of kafka direct groupId all on kafka broker direct.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-18
 */
func listKafkaGroupIdAll() []string {
	groupIds := make([]string, 0)
	for _, broker := range listBrokers() {
		for _, groupId := range listKafkaGroupId(broker) {
			groupIds = append(groupIds, groupId)
		}
	}
	return groupIds
}

/**
 * List of kafka direct groupId on kafka broker direct.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func listKafkaGroupId(broker *sarama.Broker) []string {
	if err := broker.Open(opt.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
		common.ErrorExit(err, "Cannot connect to brokerID: %d, %s", broker.ID())
	}

	// Get groupIds.
	groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		common.ErrorExit(err, "Cannot get kafka groups.")
	}
	groupIds := make([]string, 0)
	for groupId := range groups.Groups {
		groupIds = append(groupIds, groupId)
	}
	return groupIds
}

/**
 * List of topics on kafka broker direct.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-20
 */
func listTopicAll() []string {
	var topics, err = opt.client.Topics()
	if err != nil {
		common.ErrorExit(err, "Cannot get topics.")
	}
	//log.Printf("Got size of topics: %d", len(topics))
	return topics
}

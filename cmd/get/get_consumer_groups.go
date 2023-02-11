package get

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ConsumerGroup contains information displayed by "kafkactl get consumer".
type ConsumerGroup struct {
	GroupID         string
	Protocol        string        `table:"-"`
	ProtocolType    string        `table:"-"`
	State           string        `table:"-"`
	CoordinatorID   int32         `table:"-"`
	CoordinatorAddr string        `table:"-"`
	Members         []GroupMember `table:"-"`
	Offsets         []GroupOffset `table:"-"`
	Clients         string        `json:"-" yaml:"-" table:"CLIENTS"`
	OffsetsSummary  string        `json:"-" yaml:"-" table:"OFFSETS"`
}

// GroupMember contains information displayed by "kafkactl get consumer".
type GroupMember struct {
	ID         string
	ClientID   string
	ClientHost string
	Topics     []string
	UserData   json.RawMessage
}

// GroupOffset contains information displayed by "kafkactl get consumer".
type GroupOffset struct {
	Topic               string
	Partition           int32
	LastCommittedOffset int64
	HighWaterMark       int64 // the offset of the last message that was successfully copied to all of the log’s replicas
	PendingMessages     int64
}

func (cmd *command) GetConsumerGroupsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "consumers [name]",
		Args:    cobra.MaximumNArgs(1),
		Aliases: []string{"consumer", "consumer-groups", "consumer-group"},
		Short:   "List all consumer groups or display information only for a specific consumer group",
		RunE: func(_ *cobra.Command, args []string) error {
			var name string
			if len(args) > 0 {
				name = args[0]
			}
			encoding := viper.GetString("output")
			return cmd.getConsumerGroups(name, encoding)
		},
	}
}

func (cmd *command) getConsumerGroups(name, encoding string) error {
	if name == "" {
		return cmd.listGroups(encoding)
	}

	return cmd.getConsumerGroup(name, encoding)
}

func (cmd *command) listGroups(encoding string) error {
	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	resp, err := admin.ListConsumerGroups()
	if err != nil {
		return err
	}

	var groups []string
	for name := range resp {
		groups = append(groups, name)
	}

	sort.Strings(groups)

	return cli.Print(encoding, groups)
}

func (cmd *command) getConsumerGroup(groupID, encoding string) error {
	conf := cmd.SaramaConfig()
	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return err
	}

	defer client.Close()

	coordinator, err := client.Coordinator(groupID)
	if err != nil {
		return fmt.Errorf("failed to determine consumer group coordinator: %w", err)
	}

	cmd.debug.Printf("Retrieving consumer meta data for group %q", groupID)
	metadataReq := &sarama.ConsumerMetadataRequest{ConsumerGroup: groupID}
	metadataResp, err := coordinator.GetConsumerMetadata(metadataReq)
	if err != nil {
		return fmt.Errorf("failed to get consumer meta data: %w", err)
	}
	if metadataResp.Err != sarama.ErrNoError {
		return metadataResp.Err
	}

	cmd.debug.Printf("Fetching group description for %q", groupID)
	describeReq := &sarama.DescribeGroupsRequest{Groups: []string{groupID}}
	describeResp, err := coordinator.DescribeGroups(describeReq)
	if err != nil {
		return fmt.Errorf("failed to describe groups: %w", err)
	}
	if len(describeResp.Groups) != 1 {
		return fmt.Errorf("unexpected number of groups in Kafka response: want 1 but got %d", len(describeResp.Groups))
	}
	g := describeResp.Groups[0]
	if g.Err != sarama.ErrNoError {
		return fmt.Errorf("group description error: %w", g.Err)
	}

	description := ConsumerGroup{
		CoordinatorID:   coordinator.ID(),
		CoordinatorAddr: coordinator.Addr(),
		GroupID:         g.GroupId,
		Protocol:        g.ProtocolType,
		ProtocolType:    g.ProtocolType,
		State:           g.State,
	}

	for id, m := range g.Members {
		mem := GroupMember{
			ID:         id,
			ClientID:   m.ClientId,
			ClientHost: m.ClientHost,
		}

		meta, err := m.GetMemberMetadata()
		if err != nil {
			return fmt.Errorf("invalid member meta data in client %q: %w", id, err)
		}

		for _, t := range meta.Topics {
			if isIgnoredTopic(t) {
				continue
			}

			mem.Topics = append(mem.Topics, t)
			mem.UserData = meta.UserData
		}

		description.Members = append(description.Members, mem)
		description.Clients += mem.ClientID + ","
	}
	description.Clients = strings.Trim(description.Clients, ",")

	var topics []string
	if t := viper.GetString("topic"); t != "" {
		topics = []string{t}
	} else {
		cmd.debug.Println("Fetching all topics and partitions")
		topics, err = client.Topics()
		if err != nil {
			return fmt.Errorf("failed to list topics: %w", err)
		}
	}

	topicPartitions := map[string][]int32{}
	for _, topic := range topics {
		topicPartitions[topic], err = client.Partitions(topic)
		if err != nil {
			return fmt.Errorf("failed to get partitions of topic %q: %w", topic, err)
		}
	}

	cmd.debug.Println("Fetching topic offsets")
	partitionOffsets := map[string]map[int32]int64{}
	for topic, partitions := range topicPartitions {
		topicOffsets := map[int32]int64{}
		for _, partition := range partitions {
			topicOffsets[partition], err = client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return fmt.Errorf("dailed to fetch offset for partition %d of topic %q: %w", partition, topic, err)
			}
		}
		partitionOffsets[topic] = topicOffsets
	}

	cmd.debug.Println("Fetching consumer group offsets")
	groupOffsetReq := &sarama.OffsetFetchRequest{Version: 1, ConsumerGroup: groupID}
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			groupOffsetReq.AddPartition(topic, partition)
		}
	}

	groupOffsetResp, err := coordinator.FetchOffset(groupOffsetReq)
	if err != nil {
		return fmt.Errorf("failed to fetch consumer group offsets: %w", err)
	}

	description.Offsets = []GroupOffset{}
	for topic, block := range groupOffsetResp.Blocks {
		for partition, m := range block {
			topicOffsets, ok := partitionOffsets[topic]
			if !ok {
				return fmt.Errorf("offsets for topic %q have not been returned from c.GetOffset", topic)
			}
			partitionOffset, ok := topicOffsets[partition]
			if !ok {
				return fmt.Errorf("oartition offset for partition %d of topic %q has not been returned from c.GetOffset", partition, topic)
			}

			if m.Offset == -1 {
				// This group has never committed anything to this topic partition
				continue
			}

			description.Offsets = append(description.Offsets, GroupOffset{
				Topic:               topic,
				Partition:           partition,
				LastCommittedOffset: m.Offset,
				HighWaterMark:       partitionOffset,
				PendingMessages:     partitionOffset - m.Offset,
			})
		}
	}

	for _, o := range description.Offsets {
		description.OffsetsSummary += fmt.Sprintf("%s:%d:%d, ", o.Topic, o.LastCommittedOffset, o.HighWaterMark)
	}

	description.OffsetsSummary = strings.Trim(description.OffsetsSummary, ", ")

	sort.Slice(description.Offsets, func(i, j int) bool {
		if description.Offsets[i].Topic == description.Offsets[j].Topic {
			return description.Offsets[i].Partition < description.Offsets[j].Partition
		}

		return description.Offsets[i].Topic < description.Offsets[j].Topic
	})

	return cli.Print(encoding, description)
}

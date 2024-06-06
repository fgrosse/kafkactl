package get

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ConsumerGroup contains information displayed by "kafkactl get consumer".
type ConsumerGroup struct {
	GroupID         string        `table:"GROUP_ID"`
	Protocol        string        `table:"-"`
	ProtocolType    string        `table:"-"`
	State           string        `table:"-"`
	CoordinatorID   int32         `table:"-"`
	CoordinatorAddr string        `table:"-"`
	Members         []GroupMember `table:"-"`
	Offsets         []GroupOffset `table:"-"`
	Clients         string        `table:"-" json:"-" yaml:"-"`
	OffsetsSummary  string        `table:"-" json:"-" yaml:"-"`
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
	getConsumerGroupsCmd := &cobra.Command{
		Use:     "consumer-group [name]...",
		Args:    cobra.MaximumNArgs(1),
		Aliases: []string{"consumer-groups", "consumer", "consumers", "group", "groups"},
		Short:   "List all consumer groups or display information only for a specific consumer group",
		Example: `
  # Show a table of all consumer groups
  kafkactl get consumer-group

  # Show only the consumers named "example1" and example"3"
  kafkactl get consumer-group example1 example3

  # Show all information about a specific consumer group as JSON
  kafkactl get consumer-group "example-consumer" -o json`,
		RunE: func(_ *cobra.Command, args []string) error {
			var name string
			if len(args) > 0 {
				name = args[0]
			}

			fetchOffsets := viper.GetBool("fetch-offsets")
			topicFilter := viper.GetString("topic")

			regex := viper.GetString("regex")
			encoding := viper.GetString("output")
			return cmd.getConsumerGroups(name, regex, fetchOffsets, topicFilter, encoding)
		},
	}

	flags := getConsumerGroupsCmd.Flags()
	flags.StringP("regex", "e", "", "only show groups which match this regular expression")
	flags.Bool("fetch-offsets", false, `show consumer group topic offsets (slow on large clusters)`)
	flags.String("topic", "", `show topic offsets for a specific topic only (faster on large clusters)`)

	return getConsumerGroupsCmd
}

func (cmd *command) getConsumerGroups(name, regex string, fetchOffsets bool, topicFilter, encoding string) error {
	var rexp *regexp.Regexp
	if regex != "" {
		var err error
		rexp, err = regexp.Compile(regex)
		if err != nil {
			return fmt.Errorf("failed to compile regular expression: %w", err)
		}
	}

	conf, err := cmd.SaramaConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return err
	}

	defer client.Close()

	var groups []string
	if name == "" {
		var err error
		groups, err = cmd.listGroups(rexp)
		if err != nil {
			return fmt.Errorf("failed to list all consumer groups: %w", err)
		}
	} else {
		groups = []string{name}
	}

	groupDescriptions, err := admin.DescribeConsumerGroups(groups)
	if err != nil {
		return fmt.Errorf("failed to describe consumer groups: %w", err)
	}

	var result []ConsumerGroup
	for _, group := range groups {
		g, err := cmd.getConsumerGroup(client, group, groupDescriptions, fetchOffsets, topicFilter)
		if err != nil {
			return err
		}
		result = append(result, g)
	}

	return cli.Print(encoding, result)
}

func (cmd *command) listGroups(regex *regexp.Regexp) ([]string, error) {
	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return nil, err
	}

	defer admin.Close()

	resp, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}

	var groups []string
	for name := range resp {
		if regex != nil && !regex.MatchString(name) {
			continue
		}

		groups = append(groups, name)
	}

	sort.Strings(groups)
	return groups, nil
}

func (cmd *command) getConsumerGroup(client sarama.Client, groupID string, groupDescriptions []*sarama.GroupDescription, fetchOffsets bool, topicFilter string) (ConsumerGroup, error) {
	var description ConsumerGroup

	cmd.debug.Printf("Determining coordinator for group %q", groupID)
	coordinator, err := client.Coordinator(groupID)
	if err != nil {
		return description, fmt.Errorf("failed to determine consumer group coordinator: %w", err)
	}

	description.CoordinatorID = coordinator.ID()
	description.CoordinatorAddr = coordinator.Addr()

	var g *sarama.GroupDescription
	for _, descr := range groupDescriptions {
		if descr.GroupId == groupID {
			g = descr
			break
		}
	}

	if g == nil {
		return description, fmt.Errorf("missing group description for group %q", groupID)
	}

	description.GroupID = g.GroupId
	description.Protocol = g.ProtocolType
	description.ProtocolType = g.ProtocolType
	description.State = g.State

	for id, m := range g.Members {
		mem := GroupMember{
			ID:         id,
			ClientID:   m.ClientId,
			ClientHost: m.ClientHost,
		}

		meta, err := m.GetMemberMetadata()
		if err != nil {
			cmd.logger.Printf("ERROR: invalid member meta data in client %q: %s", id, err)
			continue
		}
		if meta == nil {
			cmd.logger.Printf("ERROR: member metadata is nil for group %q", groupID)
			continue
		}

		for _, t := range meta.Topics {
			if isInternalTopic(t) {
				continue
			}

			mem.Topics = append(mem.Topics, t)
			// mem.UserData = meta.UserData
		}

		description.Members = append(description.Members, mem)
		description.Clients += mem.ClientID + ","
	}
	description.Clients = strings.Trim(description.Clients, ",")

	if !fetchOffsets {
		return description, nil
	}

	var topics []string
	if topicFilter != "" {
		topics = []string{topicFilter}
	} else {
		// TODO: only fetch topics consumed by this group
		cmd.debug.Println("Fetching all topics and partitions")
		topics, err = client.Topics()
		if err != nil {
			return description, fmt.Errorf("failed to list topics: %w", err)
		}
	}

	topicPartitions := map[string][]int32{}
	for _, topic := range topics {
		topicPartitions[topic], err = client.Partitions(topic)
		if err != nil {
			return description, fmt.Errorf("failed to get partitions of topic %q: %w", topic, err)
		}
	}

	cmd.debug.Println("Fetching topic offsets") // TODO: possible in a single request?
	partitionOffsets := map[string]map[int32]int64{}
	for topic, partitions := range topicPartitions {
		topicOffsets := map[int32]int64{}
		for _, partition := range partitions {
			topicOffsets[partition], err = client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return description, fmt.Errorf("dailed to fetch offset for partition %d of topic %q: %w", partition, topic, err)
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
		return description, fmt.Errorf("failed to fetch consumer group offsets: %w", err)
	}

	description.Offsets = []GroupOffset{}
	for topic, block := range groupOffsetResp.Blocks {
		for partition, m := range block {
			topicOffsets, ok := partitionOffsets[topic]
			if !ok {
				return description, fmt.Errorf("offsets for topic %q have not been returned from c.GetOffset", topic)
			}
			partitionOffset, ok := topicOffsets[partition]
			if !ok {
				return description, fmt.Errorf("oartition offset for partition %d of topic %q has not been returned from c.GetOffset", partition, topic)
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
		description.OffsetsSummary += fmt.Sprintf("%s:%d=%d, ", o.Topic, o.Partition, o.LastCommittedOffset)
	}

	description.OffsetsSummary = strings.Trim(description.OffsetsSummary, ", ")

	sort.Slice(description.Offsets, func(i, j int) bool {
		if description.Offsets[i].Topic == description.Offsets[j].Topic {
			return description.Offsets[i].Partition < description.Offsets[j].Partition
		}

		return description.Offsets[i].Topic < description.Offsets[j].Topic
	})

	return description, nil
}

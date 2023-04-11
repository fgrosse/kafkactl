package get

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Topic contains information displayed by "get topic".
type Topic struct {
	Name              string
	Partitions        []PartitionMetadata `table:"-"`
	ConsumerGroups    []string            `table:"-"`
	NumPartitions     int32               `json:"-" yaml:"-" table:"PARTITIONS"`
	ReplicationFactor int16               `table:"REPLICATION"`
	Retention         string              `table:"RETENTION"`
	Configuration     map[string]*string  `table:"-"`
}

// PartitionMetadata contains information displayed by "get topic".
type PartitionMetadata struct {
	PartitionID     int32
	Leader          int32
	Offset          int64
	Replicas        []int32
	InSyncReplicas  []int32
	OfflineReplicas []int32
}

func (cmd *command) GetTopicsCmd() *cobra.Command {
	getTopicsCmd := &cobra.Command{
		Use:     "topics [name]",
		Aliases: []string{"topic"},
		Short:   "List one or many topics",
		Long: "List all topics or fetch information for only a subset of topics " +
			"by passing the respective topic name as arguments",
		Args: cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			showAll := viper.GetBool("all")
			regex := viper.GetString("regex")
			encoding := viper.GetString("output")
			return cmd.getTopics(showAll, regex, encoding, args)
		},
	}

	flags := getTopicsCmd.Flags()
	flags.BoolP("all", "a", false, `show also Kafka internal topics (e.g. "__consumer_offsets")`)
	flags.StringP("regex", "e", "", "only show topics which match this regular expression")

	return getTopicsCmd
}

func (cmd *command) getTopics(showAll bool, regex, encoding string, args []string) error {
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

	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return err
	}

	defer client.Close()

	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	withConsumers := true
	if encoding == "table" {
		withConsumers = false
	}

	topics, err := cmd.fetchTopics(client, admin, args, showAll, rexp, withConsumers)
	if err != nil {
		return err
	}

	return cli.Print(encoding, topics)
}

// FetchTopics will return a list of topics and their metadata, either provide an
// empty set for all topics or one or more names to get information on specific
// topics. Pass regexEnabled=true to parse the first topicsArgs element as a
// regex. If showAll=true internal kafka topics will be displayed.
func (cmd *command) fetchTopics(client sarama.Client, admin sarama.ClusterAdmin, topicsArgs []string, showAll bool, regex *regexp.Regexp, withConsumers bool) ([]Topic, error) {
	topicDetails, err := cmd.fetchTopicMetaData(client, topicsArgs)
	if err != nil {
		return nil, err
	}

	// TODO: re-enable or remove regex feature
	//       To do this we should split the calls to first fetch really only
	//       topic names, then filter them with the regexp and only then make
	//       the more expensive calls to fetch meta data
	var topics []Topic
	for topicName, details := range topicDetails {
		if !showAll && isIgnoredTopic(topicName) {
			continue
		}

		if regex != nil && !regex.MatchString(topicName) {
			continue
		}

		top := Topic{
			Name:              topicName,
			NumPartitions:     details.NumPartitions,
			ReplicationFactor: details.ReplicationFactor,
			Configuration:     details.ConfigEntries,
		}

		top.Retention = cmd.getTopicRetention(details)
		top.Partitions = cmd.fetchPartitionsOffsets(client, topicName, details)

		topics = append(topics, top)
	}

	if withConsumers {
		err = cmd.assignTopicConsumers(admin, topics)
		if err != nil {
			return nil, err
		}
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	return topics, nil
}

type TopicDetail struct {
	sarama.TopicDetail
	Partitions []*sarama.PartitionMetadata
}

func (*command) fetchTopicMetaData(client sarama.Client, topics []string) (map[string]TopicDetail, error) {
	brokers := client.Brokers()
	if len(brokers) == 0 {
		return nil, errors.New("no available broker")
	}

	b := brokers[rand.Intn(len(brokers))]
	err := b.Open(client.Config())
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to broker")
	}

	req := &sarama.MetadataRequest{Topics: topics}
	metaDataResp, err := b.GetMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster meta data: %w", err)
	}

	topicMeta := map[string]TopicDetail{}
	var describeConfigsResources []*sarama.ConfigResource
	for _, meta := range metaDataResp.Topics {
		details := sarama.TopicDetail{
			NumPartitions: int32(len(meta.Partitions)),
		}

		if len(meta.Partitions) > 0 {
			details.ReplicaAssignment = map[int32][]int32{}
			for _, partition := range meta.Partitions {
				details.ReplicaAssignment[partition.ID] = partition.Replicas
			}
			details.ReplicationFactor = int16(len(meta.Partitions[0].Replicas))
		}

		topicMeta[meta.Name] = TopicDetail{
			TopicDetail: details,
			Partitions:  meta.Partitions,
		}

		// we populate the resources we want to describe from the MetadataResponse
		describeConfigsResources = append(describeConfigsResources, &sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: meta.Name,
		})
	}

	// Send the DescribeConfigsRequest
	describeConfigsReq := &sarama.DescribeConfigsRequest{
		Resources: describeConfigsResources,
	}
	describeConfigsResp, err := b.DescribeConfigs(describeConfigsReq)
	if err != nil {
		return nil, err
	}

	for _, resource := range describeConfigsResp.Resources {
		topicDetails := topicMeta[resource.Name]
		topicDetails.ConfigEntries = make(map[string]*string)

		for _, entry := range resource.Configs {
			// only include non-default non-sensitive config
			// (don't actually think topic config will ever be sensitive)
			if entry.Default || entry.Sensitive {
				continue
			}
			topicDetails.ConfigEntries[entry.Name] = &entry.Value
		}

		topicMeta[resource.Name] = topicDetails
	}

	return topicMeta, nil
}

// special meta topic such as "__consumer_offsets"
func isIgnoredTopic(name string) bool {
	return strings.HasPrefix(name, "_")
}

func (cmd *command) fetchPartitionsOffsets(client sarama.Client, topicName string, details TopicDetail) []PartitionMetadata {
	result := make([]PartitionMetadata, details.NumPartitions)
	for i, p := range details.Partitions {
		offset, err := client.GetOffset(topicName, p.ID, sarama.OffsetNewest)
		if err != nil {
			cmd.logger.Printf("WARNING: Failed to fetch offset for topic %q partition %d: %v", topicName, p.ID, err)
			continue
		}

		sort.Slice(p.Replicas, func(i, j int) bool { return p.Replicas[i] < p.Replicas[j] })
		sort.Slice(p.Isr, func(i, j int) bool { return p.Isr[i] < p.Isr[j] })
		sort.Slice(p.OfflineReplicas, func(i, j int) bool { return p.OfflineReplicas[i] < p.OfflineReplicas[j] })

		result[i] = PartitionMetadata{
			PartitionID:     p.ID,
			Offset:          offset,
			Leader:          p.Leader,
			Replicas:        p.Replicas,
			InSyncReplicas:  p.Isr,
			OfflineReplicas: p.OfflineReplicas,
		}
	}

	return result
}

func (cmd *command) assignTopicConsumers(admin sarama.ClusterAdmin, topics []Topic) error {
	topicConsumers, err := cmd.fetchTopicConsumers(admin, topics)
	if err != nil {
		return err
	}

	for i, topic := range topics {
		groups, ok := topicConsumers[topic.Name]
		if !ok {
			continue
		}

		topic.ConsumerGroups = groups
		topics[i] = topic
	}

	return nil
}

func (*command) fetchTopicConsumers(admin sarama.ClusterAdmin, topics []Topic) (map[string][]string, error) {
	consumerGroups, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	topicOffsets := map[string]map[int32]int64{}
	topicPartitions := map[string][]int32{}
	for _, t := range topics {
		topicPartitions[t.Name] = make([]int32, t.NumPartitions)
		topicOffsets[t.Name] = map[int32]int64{}
		for i, p := range t.Partitions {
			topicPartitions[t.Name][i] = p.PartitionID
			topicOffsets[t.Name][p.PartitionID] = p.Offset
		}
	}

	topicConsumers := map[string][]string{}
	for group := range consumerGroups {
		offsets, err := admin.ListConsumerGroupOffsets(group, topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to list consumer group offsets: %w", err)
		}

		for topic, partitions := range topicPartitions {
			for _, partition := range partitions {
				block := offsets.GetBlock(topic, partition)
				if block.Offset != -1 {
					topicConsumers[topic] = append(topicConsumers[topic], group)
					break
				}
			}
		}
	}

	return topicConsumers, nil
}

func (*command) getTopicRetention(details TopicDetail) string {
	r := details.ConfigEntries["retention.ms"]
	if r == nil {
		return ""
	}

	d, err := time.ParseDuration(*r + "ms")
	if err != nil {
		return ""
	}

	return shortDuration(d)
}

func shortDuration(d time.Duration) string {
	if d == 0 {
		return "-"
	}

	d = d.Round(time.Minute)
	hours := d.Hours()
	minutes := (hours - float64(int(hours))) * 60

	// if we have more than a week and a round number of weeks then output simpler format
	if hours > 24*7 && math.Mod(hours, 24*7) == 0 {
		return fmt.Sprintf("%d weeks", int(hours/(24*7)))
	}

	// if we have more than 24h and a round number of days then output simpler format
	if hours > 24 && math.Mod(hours, 24) == 0 {
		return fmt.Sprintf("%d days", int(hours/24))
	}

	// if we have less than 24h and a round number of hours then output simpler format
	if hours <= 24 && minutes == 0 {
		return fmt.Sprintf("%d hours", int(hours))
	}

	// if we have less than 1h, display minutes only
	if int(hours) == 0 {
		return fmt.Sprintf("%dm", int(minutes))
	}

	// display minutes and hours
	return fmt.Sprintf("%dh%dm", int(hours), int(minutes))
}

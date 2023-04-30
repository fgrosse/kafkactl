package get

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"sync"
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
			showInternal := viper.GetBool("all")
			showConsumers := viper.GetBool("with-consumers")
			regex := viper.GetString("regex")
			encoding := viper.GetString("output")
			return cmd.getTopics(showInternal, showConsumers, regex, encoding, args)
		},
	}

	flags := getTopicsCmd.Flags()
	flags.BoolP("all", "a", false, `show also Kafka internal topics (e.g. "__consumer_offsets")`)
	flags.Bool("with-consumers", false, `show consumer group offsets. This needs to fetch metadata for all consumer groups in your cluster and can be very slow!`)
	flags.StringP("regex", "e", "", "only show topics which match this regular expression")

	return getTopicsCmd
}

func (cmd *command) getTopics(showInternal, showConsumers bool, regex, encoding string, args []string) error {
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

	showOffsets := true
	if encoding == "table" {
		showOffsets = false
	}

	topics, err := cmd.fetchTopics(client, args, rexp, showInternal, showConsumers, showOffsets)
	if err != nil {
		return err
	}

	return cli.Print(encoding, topics)
}

// fetchTopics will return a list of topics and their metadata. You can provide
// a list of topic names to show only specific topics. If the list is empty,
// all topics will be displayed. Pass a regular expression to filter out all
// topics that don't match it.
func (cmd *command) fetchTopics(client sarama.Client, topicsNames []string, regex *regexp.Regexp, showInternal, showConsumers, showOffsets bool) ([]Topic, error) {
	topics, err := cmd.fetchTopicMetaData(client, topicsNames, regex, showInternal)
	if err != nil {
		return nil, err
	}

	var result []Topic
	for topicName, topic := range topics {
		t := Topic{
			Name:              topicName,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
			Configuration:     topic.ConfigEntries,
			Retention:         cmd.parseTopicRetention(topic),
		}

		if showOffsets {
			t.Partitions = cmd.fetchPartitionsOffsets(client, topicName, topic)
		}

		result = append(result, t)
	}

	if showConsumers {
		err = cmd.assignTopicConsumers(result)
		if err != nil {
			return nil, err
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

type TopicDetail struct {
	sarama.TopicDetail
	Partitions []*sarama.PartitionMetadata
}

func (cmd *command) fetchTopicMetaData(client sarama.Client, topics []string, regex *regexp.Regexp, showInternal bool) (map[string]TopicDetail, error) {
	brokers := client.Brokers()
	if len(brokers) == 0 {
		return nil, errors.New("no available broker")
	}

	b := brokers[rand.Intn(len(brokers))]
	err := b.Open(client.Config())
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to broker")
	}

	req := &sarama.MetadataRequest{
		Topics:                 topics,
		AllowAutoTopicCreation: false,
	}

	cmd.debug.Println("Requesting topic metadata")
	start := time.Now()
	resp, err := b.GetMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster meta data: %w", err)
	}
	cmd.debug.Printf("Topic metadata request took %s", time.Since(start))

	topicMeta := map[string]TopicDetail{}
	var resources []*sarama.ConfigResource
	for _, meta := range resp.Topics {
		if !showInternal && isInternalTopic(meta.Name) {
			continue
		}
		if regex != nil && !regex.MatchString(meta.Name) {
			continue
		}

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

		resources = append(resources, &sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: meta.Name,
		})
	}

	err = cmd.fetchTopicConfiguration(b, resources, topicMeta)
	if err != nil {
		return nil, err
	}

	return topicMeta, nil
}

func (cmd *command) fetchTopicConfiguration(b *sarama.Broker, resources []*sarama.ConfigResource, topicMeta map[string]TopicDetail) error {
	req := &sarama.DescribeConfigsRequest{
		Resources: resources,
	}

	cmd.debug.Println("Requesting topic configuration")
	start := time.Now()
	resp, err := b.DescribeConfigs(req)
	if err != nil {
		return err
	}
	cmd.debug.Printf("Topic configuration request took %s", time.Since(start))

	for _, resource := range resp.Resources {
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

	return nil
}

// special meta topic such as "__consumer_offsets"
func isInternalTopic(name string) bool {
	return strings.HasPrefix(name, "_")
}

func (cmd *command) fetchPartitionsOffsets(client sarama.Client, topicName string, details TopicDetail) []PartitionMetadata {
	offsets := make(chan *PartitionMetadata, len(details.Partitions))

	start := time.Now()
	cmd.debug.Printf("Fetching all partition offsets")
	result := make([]PartitionMetadata, 0, details.NumPartitions)

	wg := sync.WaitGroup{}
	for _, p := range details.Partitions {
		wg.Add(1)
		go func(partition *sarama.PartitionMetadata) {
			defer wg.Done()
			meta, err := cmd.fetchPartitionsOffset(client, topicName, p)
			if err != nil {
				cmd.logger.Printf("ERROR: Failed to fetch offset for topic %q partition %d: %v", topicName, p.ID, err)
				return
			}

			offsets <- meta
		}(p)
	}

	go func() {
		wg.Wait()
		close(offsets)
	}()

	for meta := range offsets {
		result = append(result, *meta)
	}

	cmd.debug.Printf("Fetching all partition offsets took %s", time.Since(start))
	return result
}

func (cmd *command) fetchPartitionsOffset(client sarama.Client, topicName string, partition *sarama.PartitionMetadata) (*PartitionMetadata, error) {
	cmd.debug.Printf("Fetching partition offsets. topic=%s partition=%d", topicName, partition.ID)
	start := time.Now()
	offset, err := client.GetOffset(topicName, partition.ID, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}
	cmd.debug.Printf("Fetching partition offset took %s", time.Since(start))

	sort.Slice(partition.Replicas, func(i, j int) bool { return partition.Replicas[i] < partition.Replicas[j] })
	sort.Slice(partition.Isr, func(i, j int) bool { return partition.Isr[i] < partition.Isr[j] })
	sort.Slice(partition.OfflineReplicas, func(i, j int) bool { return partition.OfflineReplicas[i] < partition.OfflineReplicas[j] })

	return &PartitionMetadata{
		PartitionID:     partition.ID,
		Offset:          offset,
		Leader:          partition.Leader,
		Replicas:        partition.Replicas,
		InSyncReplicas:  partition.Isr,
		OfflineReplicas: partition.OfflineReplicas,
	}, nil
}

func (cmd *command) assignTopicConsumers(topics []Topic) error {
	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

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

func (cmd *command) fetchTopicConsumers(admin sarama.ClusterAdmin, topics []Topic) (map[string][]string, error) {
	cmd.debug.Println("Listing all consumer groups")
	start := time.Now()
	consumerGroups, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	cmd.debug.Printf("Found %d consumer groups. Took=%s", len(consumerGroups), time.Since(start))

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
		cmd.debug.Printf("Fetching consumer group offset for %d topics. group=%s", len(topicPartitions), group)
		start = time.Now()
		offsets, err := admin.ListConsumerGroupOffsets(group, topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to list consumer group offsets: %w", err)
		}
		cmd.debug.Printf("Fetching consumer group offset took %s", time.Since(start))

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

func (*command) parseTopicRetention(details TopicDetail) string {
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

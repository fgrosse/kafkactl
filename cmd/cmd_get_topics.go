package cmd

import (
	"fmt"
	"log"
	"math"
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
	Consumers         []string            `table:"-"`
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

func (cmd *Kafkactl) GetTopicsCmd() *cobra.Command {
	getTopicsCmd := &cobra.Command{
		Use:     "topics [TOPIC_NAME]",
		Aliases: []string{"topic"},
		Short:   "List one or many topics",
		Long: "List all topics or fetch information for only a subset of topics " +
			"by passing the respective topic name as arguments",
		Args: cobra.ArbitraryArgs,
		RunE: cmd.runGetTopicsCmd,
	}

	flags := getTopicsCmd.Flags()
	flags.BoolP("all", "a", false, "show also Kafka internal topics")
	flags.StringP("regex", "e", "", "only show topics which match this regular expression")
	flags.BoolP("fetch-metadata", "m", true, "fetch metadata such as partitions and their offsets")

	return getTopicsCmd
}

func (cmd *Kafkactl) runGetTopicsCmd(_ *cobra.Command, args []string) error {
	showAll := viper.GetBool("all")
	regex := viper.GetString("regex")
	fetchMetadata := viper.GetBool("fetch-metadata")
	outputEncoding := viper.GetString("output")

	var rexp *regexp.Regexp
	if regex != "" {
		var err error
		rexp, err = regexp.Compile(regex)
		if err != nil {
			return fmt.Errorf("failed to compile regular expression: %w", err)
		}
	}

	client, err := cmd.connectClient()
	if err != nil {
		return err
	}

	defer client.Close()

	admin, err := cmd.connectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	topics, err := FetchTopics(client, admin, args, fetchMetadata, showAll, rexp)
	if err != nil {
		return err
	}

	return cli.Print(outputEncoding, topics)
}

// FetchTopics will return a list of topics and their metadata, either provide an
// empty set for all topics or one or more names to get information on specific
// topics. Pass regexEnabled=true to parse the first topicsArgs element as a
// regex. If showAll=true internal kafka topics will be displayed.
func FetchTopics(client sarama.Client, admin sarama.ClusterAdmin, topicsArgs []string, fetchMetadata, showAll bool, regex *regexp.Regexp) ([]Topic, error) {
	var (
		metaDataResp *sarama.MetadataResponse
		err          error
	)

	for _, b := range client.Brokers() {
		err = b.Open(client.Config())
		if err != nil {
			continue // TODO
		}

		req := &sarama.MetadataRequest{Topics: topicsArgs}
		metaDataResp, err = b.GetMetadata(req)
		if err != nil {
			continue // TODO
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get cluster meta data: %w", err)
	}

	topicsResponse, err := admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	topicMeta := map[string]*sarama.TopicMetadata{}
	for _, meta := range metaDataResp.Topics {
		topicMeta[meta.Name] = meta
	}

	var topics []Topic
	for topicName, details := range topicsResponse {
		if !showAll && isIgnoredTopic(topicName) {
			continue
		}

		if _, ok := topicMeta[topicName]; !ok {
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

		if !fetchMetadata {
			topics = append(topics, top)
			continue
		}

		retention := getTopicRetention(details)
		top.Retention = shortDuration(retention)

		meta := topicMeta[topicName]
		if meta == nil {
			log.Printf("WARNING: Did not find meta data for topic %q", topicName)
			continue
		}

		top.Partitions = make([]PartitionMetadata, len(meta.Partitions))
		for i, p := range meta.Partitions {
			offset, err := client.GetOffset(top.Name, p.ID, sarama.OffsetNewest)
			if err != nil {
				log.Printf("WARNING: Failed to fetch offset for topic %q partition %d: %v", topicName, p.ID, err)
			}

			sort.Slice(p.Replicas, func(i, j int) bool { return p.Replicas[i] < p.Replicas[j] })
			sort.Slice(p.Isr, func(i, j int) bool { return p.Isr[i] < p.Isr[j] })
			sort.Slice(p.OfflineReplicas, func(i, j int) bool { return p.OfflineReplicas[i] < p.OfflineReplicas[j] })

			top.Partitions[i] = PartitionMetadata{
				PartitionID:     p.ID,
				Offset:          offset,
				Leader:          p.Leader,
				Replicas:        p.Replicas,
				InSyncReplicas:  p.Isr,
				OfflineReplicas: p.OfflineReplicas,
			}
		}

		topics = append(topics, top)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	topicConsumers, err := fetchTopicConsumers(admin, topics)
	if err != nil {
		return nil, err
	}

	for i, topic := range topics {
		groups, ok := topicConsumers[topic.Name]
		if !ok {
			continue
		}

		topic.Consumers = groups
		topics[i] = topic
	}

	return topics, nil
}

// special meta topic such as "__consumer_offsets"
func isIgnoredTopic(name string) bool {
	return strings.HasPrefix(name, "__")
}

func fetchTopicConsumers(admin sarama.ClusterAdmin, topics []Topic) (map[string][]string, error) {
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
					consumerLag := topicOffsets[topic][partition] - block.Offset
					if consumerLag < 0 {
						// We are fetching the topic offsets before we fetch the
						// individual consumer offsets. Due to this racyness,
						// the *actual* topic offset can be higher at this point
						// which leads to negative lag values.
						consumerLag = 0
					}

					topicConsumers[topic] = append(topicConsumers[topic], group)
					break
				}
			}
		}
	}

	return topicConsumers, nil
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

	// if we have less than 1h, display minutes only
	if int(hours) == 0 {
		return fmt.Sprintf("%dm", int(minutes))
	}

	// display minutes and hours
	return fmt.Sprintf("%dh%dm", int(hours), int(minutes))
}

func getTopicRetention(details sarama.TopicDetail) time.Duration {
	r := details.ConfigEntries["retention.ms"]
	if r == nil {
		return 0
	}

	d, err := time.ParseDuration(*r + "ms")
	if err != nil {
		return 0
	}

	return d
}

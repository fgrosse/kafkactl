package create

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var validTopicName = regexp.MustCompile(`[a-zA-Z0-9._-]+`)

func (cmd *command) CreateTopicCmd() *cobra.Command {
	createTopicCmd := &cobra.Command{
		Use:     "topic <name>",
		Aliases: []string{"topics"},
		Args:    cobra.MinimumNArgs(1),
		Short:   "Create one or many topics",
		Long: `Create one or many topics

Create topics by passing at least one topic name as arguments. You can control
the amount of partitions, the replication factor and other settings using flags.

You can pass multiple topic names to create multiple topics at the same time.
All of them will have the same partition and replication settings from the flags.
`,
		RunE: func(_ *cobra.Command, args []string) error {
			names := args
			partitions := viper.GetInt32("partitions")
			replicas := int16(viper.GetInt("replicas"))
			timeout := viper.GetDuration("timeout")
			ifNotExists := viper.GetBool("if-not-exists")
			retention := viper.GetDuration("retention")
			return cmd.createTopic(names, partitions, replicas, timeout, ifNotExists, retention)
		},
	}

	flags := createTopicCmd.Flags()
	flags.Int32("partitions", 1, "Number of partitions")
	flags.Int16("replicas", 1, "Replication factor")
	flags.Duration("timeout", 30*time.Second, "Timeout for Kafka requests")
	flags.Bool("if-not-exists", false, "Do not fail if the topic already exists")
	flags.Duration("retention", 0, "Maximum time to retain messages in this topic. Leave empty for cluster default")

	return createTopicCmd
}

func (cmd *command) createTopic(
	names []string,
	partitions int32,
	replicas int16,
	timeout time.Duration,
	ignoreExistingTopics bool,
	retention time.Duration,
) error {
	req := &sarama.CreateTopicsRequest{
		Version:      0, // 0 represents v0.10.1
		TopicDetails: make(map[string]*sarama.TopicDetail),
		Timeout:      timeout,
	}

	if partitions < 1 {
		return errors.New("must have at least one partition")
	}

	if replicas < 1 {
		return errors.New("must have at least one replica")
	}

	var configs map[string]*string
	if retention > 0 {
		if retention < 30*time.Minute || retention > 31*24*time.Hour {
			return errors.New("retention must be between 30 minutes and 31 days")
		}

		retentionMillis := fmt.Sprint(int64(retention.Seconds()) * 1000)
		configs = map[string]*string{"retention.ms": &retentionMillis}
	}

	for _, topicName := range names {
		if !validTopicName.MatchString(topicName) {
			return fmt.Errorf("topic name contains invalid characters: %q", topicName)
		}

		if len(topicName) > 249 {
			return fmt.Errorf("topic name is too long: %q", topicName)
		}

		req.TopicDetails[topicName] = &sarama.TopicDetail{
			NumPartitions:     partitions,
			ReplicationFactor: replicas,
			// TODO: Replication Assignment is not implemented yet.
			// TODO: Specific custom configuration for topic is not implemented yet
			ConfigEntries: configs,
		}
	}

	conf := cmd.SaramaConfig()
	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return err
	}

	defer client.Close()

	controller, err := client.Controller()
	if err != nil {
		return err
	}
	defer controller.Close()

	resp, err := controller.CreateTopics(req)
	if err != nil {
		return fmt.Errorf("failed to create topic(s): %w", err)
	}

	for _, topicName := range names {
		if err := resp.TopicErrors[topicName].Err; err != sarama.ErrNoError {
			if ignoreExistingTopics && err == sarama.ErrTopicAlreadyExists {
				continue
			}
			return fmt.Errorf("failed to create topic %q: %w", topicName, err)
		}
	}

	cmd.logger.Printf("Topic(s) created successfully: %v", names)
	return nil
}

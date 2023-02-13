package replay

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// defaultFetchSize is a rough estimate on the size of the messages that we
// expect to be stored in Kafka.
const defaultFetchSize int32 = 1024 * 1024 // 1MB

func (cmd *command) ReplayCmd() *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   "replay",
		Args:  cobra.NoArgs,
		Short: "Read messages from a Kafka topic and append them to the end of a topic",
		Long: `Read messages from a Kafka topic and append them to the end of a topic

The replay command can be used to take an existing message in a Kafka topic and append
it to the same or another topic. This can be useful if a message was already processed
by a consumer but you want it to be processed again without resetting the entire
consumer group.

Note that you can pass one or many offsets to this command.
`,
		Example: `
  # Replay all messages of topic partition 2 up to the current last offset and then stop
  kafkactl replay --topic example-topic --partition=2 --from=oldest --until=newest

  # Dry-run a replay of specific message into the same topic
  kafkactl replay --topic example-topic --offset 42 --dry-run
  
  # Replay an offset range at once into the same topic
  kafkactl replay --topic example-topic --from 123 --until 145
  
  # Replay all messages into another Kafka cluster indefinitely
  kafkactl replay \
    --context=localhost \
    --topic=example-topic \
    --dest-context=staging \
    --dest-topic=example-topic \
    --from=oldest
	--inf
`,
		RunE: func(_ *cobra.Command, args []string) error {
			ctx := cli.Context()
			srcContext := viper.GetString("context")
			srcTopic := viper.GetString("topic")
			srcPartition := viper.GetInt32("partition")
			fromOffset := viper.GetString("from")
			untilOffset := viper.GetString("until")

			destContext := viper.GetString("dest-context")
			destTopic := viper.GetString("dest-topic")

			dryRun := viper.GetBool("dry-run")
			markReplayed := viper.GetBool("mark-replayed")
			inf := viper.GetBool("inf")

			conf := cmd.Configuration()
			if srcContext == "" {
				srcContext = conf.CurrentContext
			}
			if destContext == "" {
				destContext = conf.CurrentContext
			}
			if destTopic == "" {
				destTopic = srcTopic
			}

			if inf {
				if destTopic == srcTopic && srcContext == destContext {
					return fmt.Errorf("the destination and target topic must be different " +
						"when using --inf or you are creating an infinite message loop")
				}
				untilOffset = ""
			}

			return cmd.replay(ctx,
				srcContext, srcTopic, srcPartition,
				destContext, destTopic,
				fromOffset, untilOffset,
				dryRun, markReplayed,
			)
		},
	}

	flags := replayCmd.Flags()
	flags.String("topic", "", "topic to read messages from")
	flags.Int32("partition", 0, "partition to read messages from") // TODO: allow passing multiple times, default to all partitions
	flags.String("dest-context", "", "Kafkactl configuration context to select target brokers (defaults to current context)")
	flags.String("dest-topic", "", "topic to write messages to (defaults to --topic)")

	flags.Bool("dry-run", false, "do not actually write any messages back to Kafka")
	flags.Bool("mark-replayed", true, "mark each message as replayed via the Kafka message header (default true)")
	// flags.Int32("fetch-size", defaultFetchSize, "Kafka max fetch size")
	// flags.Bool("async", false, "use an async producer (faster)")

	// flags.StringSlice("offset", nil, "offset of messages to replay (can be passed multiple times)")
	// flags.StringSlice("offsets", nil, "a comma separated list of offsets of messages to replay (can be passed multiple times)")
	// flags.String("offsets-file", "", "a file containing one offset per line")
	flags.String("from", "oldest", `offset of first message to replay (either "oldest", "newest" or an integer)`)
	flags.String("until", "newest", `offset of last message to replay (either "newest" or an integer)`)
	flags.Bool("inf", false, "keep replaying new messages indefinitely (overriding any --until value)")

	_ = replayCmd.MarkFlagRequired("topic")

	return replayCmd
}

func (cmd *command) replay(
	ctx context.Context,
	srcContext, srcTopic string, srcPartition int32,
	destContext, destTopic string,
	fromOffset, untilOffset string,
	dryRun, markReplayed bool,
) error {
	input, err := cmd.connectSource(ctx, srcTopic, srcPartition, fromOffset, untilOffset)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	producer, err := cmd.connectDestination(destContext)
	if err != nil {
		return fmt.Errorf("failed to connect to destination: %w", err)
	}

	return cmd.sendMessages(input, producer, srcContext, destContext, destTopic, dryRun, markReplayed)
}

func (cmd *command) connectSource(ctx context.Context, topic string, partition int32, fromOffset, untilOffset string) (<-chan *sarama.ConsumerMessage, error) {
	conf := cmd.SaramaConfig()
	conf.Consumer.Return.Errors = false // TODO: log consumer errors

	src, err := cmd.ConnectClient(conf)
	if err != nil {
		return nil, err
	}

	c, err := sarama.NewConsumerFromClient(src)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	var startOffset int64
	switch fromOffset {
	case "oldest":
		startOffset = sarama.OffsetOldest
	case "newest":
		startOffset = sarama.OffsetNewest
	default:
		startOffset, err = strconv.ParseInt(fromOffset, 10, 0)
		if err != nil {
			return nil, errors.New("invalid --from value")
		}
	}

	var maxOffset int64
	switch untilOffset {
	case "":
		// there is no max offset
	case "oldest":
		return nil, fmt.Errorf("you cannot use --until=oldest")
	case "newest":
		maxOffset, err = src.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("failed to determine the offset value for --until=newest: %w", err)
		}
		maxOffset-- // GetOffset returns the most recent *available* offset, not the last one
	default:
		maxOffset, err = strconv.ParseInt(untilOffset, 10, 0)
		if err != nil {
			return nil, errors.New("invalid --until value")
		}
	}

	con, err := c.ConsumePartition(topic, partition, startOffset) // TODO: support consuming all partitions
	if err != nil {
		return nil, fmt.Errorf("failed to consume topic partition: %w", err)
	}

	if untilOffset != "" {
		cmd.logger.Printf("Consuming topic %q partition %d starting at offset %s until offset %d", topic, partition, fromOffset, maxOffset)
	} else {
		cmd.logger.Printf("Consuming topic %q partition %d starting at offset %d indefinitely", topic, partition, startOffset)
	}

	go func() {
		<-ctx.Done()
		con.AsyncClose()
	}()

	messages := make(chan *sarama.ConsumerMessage)
	go func() {
		defer close(messages)
		for msg := range con.Messages() {
			messages <- msg

			if untilOffset != "" && msg.Offset == maxOffset {
				con.Close()
				return
			}
		}
	}()

	return messages, nil
}

func (cmd *command) connectDestination(destContext string) (sarama.SyncProducer, error) {
	conf := cmd.Configuration()
	saramaConf := cmd.SaramaConfig()
	saramaConf.Producer.Return.Successes = true
	saramaConf.Producer.Return.Errors = true

	brokers := conf.Brokers(destContext)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("unknown destination context %q", destContext)
	}

	dest, err := sarama.NewClient(brokers, saramaConf)
	if err != nil {
		return nil, err
	}

	return sarama.NewSyncProducerFromClient(dest) // TODO: support async
}

func (cmd *command) sendMessages(
	input <-chan *sarama.ConsumerMessage,
	producer sarama.SyncProducer,
	srcContext, destContext, destTopic string,
	dryRun, markReplayed bool,
) error {
	var numErrors int
	for in := range input {
		if dryRun {
			cmd.logger.Printf("[DRY-RUN] Sending message offset=%d partition=%d from %s/%s to %s/%s",
				in.Offset, in.Partition, srcContext, in.Topic, destContext, destTopic,
			)
			continue
		}

		cmd.debug.Printf("Sending message offset=%d partition=%d from %s/%s to %s/%s",
			in.Offset, in.Partition, srcContext, in.Topic, destContext, destTopic,
		)

		out := &sarama.ProducerMessage{
			Topic: destTopic,
			Key:   sarama.StringEncoder(in.Key),
			Value: sarama.ByteEncoder(in.Value),
		}

		if markReplayed {
			out.Headers = append(out.Headers, sarama.RecordHeader{
				Key:   []byte("Kafkactl-Replayed"),
				Value: []byte("true"),
			})
		}

		outPartition, outOffset, err := producer.SendMessage(out)
		if err != nil {
			numErrors++
			cmd.logger.Printf("Error: failed to send message to destination topic: %w", err)
			continue
		}

		cmd.logger.Printf("Message successfully sent to Kafka "+
			"src-topic=%s src-partition=%d src-offset=%d "+
			"dest-topic=%s dest-partition=%d dest-offset=%d",
			in.Topic, in.Partition, in.Offset,
			out.Topic, outPartition, outOffset,
		)
	}

	if numErrors > 0 {
		return fmt.Errorf("there were %d errors while sending messages", numErrors)
	}

	return nil
}

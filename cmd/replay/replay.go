package replay

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/pkg"
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
  # Copy all messages of a topic to the end of the same topic
  kafkactl replay --topic example-topic

  # Dry-run a replay of specific message into the same topic
  kafkactl replay --topic example-topic --offset 42 --dry-run
  
  # Replay an offset range at once into another topic
  kafkactl replay --topic foo --dest-topic bar --from 123 --until 145
  
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
			srcPartitions := viper.GetIntSlice("partition")
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
				destContext = srcContext
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

			partitions := make([]int32, len(srcPartitions))
			for i, p := range srcPartitions {
				partitions[i] = int32(p)
			}

			return cmd.replay(ctx,
				srcContext, srcTopic, partitions,
				destContext, destTopic,
				fromOffset, untilOffset,
				dryRun, markReplayed,
			)
		},
	}

	flags := replayCmd.Flags()
	flags.String("topic", "", "topic to read messages from")
	flags.IntSlice("partition", nil, "partition to read messages from (can be passed multiple times, defaults to all partitions)")
	flags.String("dest-context", "", "Kafkactl configuration context to select target brokers (defaults to the source context)")
	flags.String("dest-topic", "", "topic to write messages to (defaults to the source topic)")

	flags.Bool("dry-run", false, "do not actually write any messages back to Kafka")
	flags.Bool("mark-replayed", true, "mark each message as replayed via the Kafka message header (default true)")
	// flags.Int32("fetch-size", defaultFetchSize, "Kafka max fetch size")
	// flags.Bool("async", false, "use an async producer (faster)")

	// flags.String("offsets-file", "", "a file containing one offset per line")
	flags.String("from", "oldest", `offset of first message to replay (either "oldest" or "newest", can be an integer when consuming only a single partition)`)
	flags.String("until", "newest", `offset of last message to replay (either "newest" or an integer when consuming only a single partition)`)
	flags.Bool("inf", false, "keep replaying new messages indefinitely (overriding any --until value)")

	_ = replayCmd.MarkFlagRequired("topic")

	return replayCmd
}

func (cmd *command) replay(
	ctx context.Context,
	srcContext, srcTopic string, srcPartition []int32,
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

func (cmd *command) connectSource(ctx context.Context, topic string, partitionIDs []int32, fromOffset, untilOffset string) (<-chan *sarama.ConsumerMessage, error) {
	client, con, err := cmd.newSourceConsumer()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if len(partitionIDs) == 0 {
		partitionIDs, err = con.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to determine amount of partitions: %w", err)
		}
	}

	startOffset, err := parseFromOffset(fromOffset, partitionIDs)
	if err != nil {
		return nil, fmt.Errorf("invalid --from value")
	}

	maxOffsets, err := parseUntilOffset(untilOffset, client, topic, partitionIDs)
	if err != nil {
		return nil, err
	}

	var partitions []pkg.PartitionOffset
	for _, partition := range partitionIDs {
		if maxOffsets[partition] == -1 {
			// We want to read up until the most recent message but this partition
			// has never received any. Therefore, we don't need to process anything.
			cmd.logger.Printf("Ignoring topic %q partition %d because it contains no messages", topic, partition)
			continue
		}

		partitions = append(partitions, pkg.PartitionOffset{
			Partition: partition,
			Offset:    startOffset,
		})
	}

	for _, p := range partitions {
		partition := p.Partition
		if untilOffset != "" {
			maxOffset := maxOffsets[partition]
			cmd.logger.Printf("Consuming topic %q partition %d starting at offset %q until offset %d", topic, partition, fromOffset, maxOffset)
		} else {
			cmd.logger.Printf("Consuming topic %q partition %d starting at offset %q indefinitely", topic, partition, fromOffset)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	messages := make(chan *sarama.ConsumerMessage, len(partitions))
	consumePartition := func(con sarama.PartitionConsumer) {
		defer wg.Done()

		for msg := range con.Messages() {
			messages <- msg

			maxOffset, ok := maxOffsets[msg.Partition]
			if ok && msg.Offset == maxOffset {
				con.Close()
				cmd.debug.Printf("Partition consumer %d has reached its maximum offset", msg.Partition)
				return
			}
		}
	}

	go func() {
		wg.Wait()
		close(messages)
	}()

	consumer := pkg.NewConsumer(con)
	err = consumer.ProcessPartitions(ctx, topic, partitions, consumePartition)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (cmd *command) newSourceConsumer() (sarama.Client, sarama.Consumer, error) {
	conf := cmd.SaramaConfig()
	conf.Consumer.Return.Errors = false // TODO: log consumer errors

	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return nil, nil, err
	}

	con, err := sarama.NewConsumerFromClient(client)
	return client, con, err
}

func parseFromOffset(offset string, partitions []int32) (int64, error) {
	switch offset {
	case "oldest":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:
		if len(partitions) != 1 {
			return 0, fmt.Errorf("when replaying messages across different partitions you cannot specify a single start offset via --from. Please use --from={oldest,newest} instead")
		}

		return strconv.ParseInt(offset, 10, 0)
	}
}

func parseUntilOffset(offset string, client sarama.Client, topic string, partitions []int32) (map[int32]int64, error) {
	switch offset {
	case "":
		// there is no max offset
		return map[int32]int64{}, nil

	case "oldest":
		return nil, fmt.Errorf("you cannot use --until=oldest")

	case "newest":
		result := map[int32]int64{}
		for _, partition := range partitions {
			maxOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, fmt.Errorf("failed to determine the offset value for --until=newest: %w", err)
			}

			result[partition] = maxOffset - 1 // GetOffset returns the most recent *available* offset, not the last one
		}
		return result, nil

	default:
		if len(partitions) != 1 {
			return nil, fmt.Errorf("when replaying messages across different partitions you cannot specify an integer max offset via --until. Please use --until=newest instead")
		}

		maxOffset, err := strconv.ParseInt(offset, 10, 0)
		if err != nil {
			return nil, errors.New("invalid --until value")
		}

		partition := partitions[0]
		return map[int32]int64{partition: maxOffset}, nil
	}
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

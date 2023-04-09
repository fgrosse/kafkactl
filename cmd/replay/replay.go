package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// defaultFetchSize is a rough estimate on the size of the messages that we
// expect to be stored in Kafka.
const defaultFetchSize int32 = 1024 * 1024 // 1MB

func (cmd *command) ReplayCmd() *cobra.Command {
	replayCmd := &cobra.Command{
		Use:     "replay",
		GroupID: "consumer-producer",
		Args:    cobra.NoArgs,
		Short:   "Read messages from a Kafka topic and append them to the end of a topic",
		Long: `Read messages from a Kafka topic and append them to the end of a topic.

The replay command can be used to take an existing message in a Kafka topic and append
it to the same or another topic. This can be useful if a message was already processed
by a consumer, but you want it to be processed again without resetting the entire
consumer group.

### Replay Source

There are various ways to control which messages should be replayed. By default,
kafkactl will replay all messages of all partitions of the source topic up to the
most recent offset when the command was started, and then stop. You can select
individual partitions using the --partition flag which can be passed multiple times. 

If you consume from a single partition, you can control the start and end offset
by using --from <offset> and --until <offset>. Alternatively, you can also
use --offset <offset1>,…,<offsetN> to replay specific offsets only.

If you want to consume specific offsets across multiple partitions, you have to
use the --offsets-file flag which accepts a JSON file in the following format:

  {
    "partitions": [
      {"partition": 0},
      {"partition": 1, "from": "123"},
      {"partition": 2, "from": "123", "to": "456"},
      {"partition": 3, "offsets": [1, 24, 321]}
    ]
  }

For the offsets file, the same precedence and defaults apply as for the flags.
That means that by default:
  * "from" is the oldest offset for that partition
  * "to" is the most recent offset for that partition at program start
  * if "offsets" is set, "from" and "to" are ignored
  * you cannot mix fetching ranges and fetching specific offsets in a single file

You can replay messages indefinitely by using the --inf flag. In that case, the
command will replay messages as described above, but when it has reached the most
recent message, it will continue to consume new messages as they come in. Note
that this flag is incompatible with --offset. Replaying messages can be aborted
by sending SIGINT, SIGTERM or SIGQUIT to the process.

### Replay Destination

By default, this command will replay messages into the same topic that it has read them
from. You can use the command flags to specify a different topic and even a different
Kafka cluster by using a different kafkactl configuration configuration as destination.

### Dry run

To test you replay logic, you can use the --dry-run flag to check that it does indeed
only replay the messages that you intended (i.e. correct partitions and offsets). 
`,
		Example: `
  # Copy all messages of a topic to the end of the same topic
  kafkactl replay --topic example-topic

  # Dry-run a replay of specific message into the same topic
  kafkactl replay --topic example-topic --partition=2 --offset 42 --dry-run
  
  # Replay an offset range at once into another topic
  kafkactl replay --topic foo --dest-topic bar --from 123 --until 145
  
  # Replay multiple specific messages across different partitions
  kafkactl replay --topic example-topic --offsets-file replay_offsets.json

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
			offsetStr := viper.GetString("offset")
			offsetsFile := viper.GetString("offsets-file")

			destContext := viper.GetString("dest-context")
			destTopic := viper.GetString("dest-topic")

			dryRun := viper.GetBool("dry-run")
			markReplayed := viper.GetBool("mark-replayed")
			inf := viper.GetBool("inf")
			fetchSize := viper.GetInt32("fetch-size")

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

			var offsets []int64
			for _, s := range strings.Split(offsetStr, ",") {
				s = strings.TrimSpace(s)
				if s == "" {
					continue
				}

				o, err := strconv.ParseInt(s, 10, 0)
				if err != nil {
					return fmt.Errorf("--offset value cannot be parsed as integer")
				}
				offsets = append(offsets, o)
			}

			if offsetsFile != "" && len(offsets) > 0 {
				return fmt.Errorf("please use either --offset or --offsets-file but not both simultaniously")
			}

			return cmd.replay(ctx,
				srcContext, srcTopic, partitions,
				destContext, destTopic,
				offsets, fromOffset, untilOffset, offsetsFile,
				dryRun, markReplayed,
				fetchSize,
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
	flags.Int32("fetch-size", defaultFetchSize, "Kafka max fetch size")
	// flags.Bool("async", false, "use an async producer (faster)")

	flags.String("from", "oldest", `offset of first message to replay (either "oldest" or "newest", can be an integer when consuming only a single partition)`)
	flags.String("until", "newest", `offset of last message to replay (either "newest" or an integer when consuming only a single partition)`)
	flags.Bool("inf", false, "keep replaying new messages indefinitely (overriding any --until value)")
	flags.String("offset", "", "a comma separated list of offsets of messages to replay (takes precedence over --from, --until or --inf)")
	flags.StringP("offsets-file", "f", "", "a JSON file containing individual offsets to replay")

	_ = replayCmd.MarkFlagRequired("topic")

	return replayCmd
}

func (cmd *command) replay(
	ctx context.Context,
	srcContext, srcTopic string, srcPartition []int32,
	destContext, destTopic string,
	offsets []int64, fromOffset, untilOffset, offsetsFile string,
	dryRun, markReplayed bool,
	fetchSize int32,
) error {
	source, err := cmd.connectSource(ctx, srcTopic, srcPartition, fromOffset, untilOffset, offsetsFile, offsets, fetchSize)
	if err != nil {
		return err
	}

	destination, err := cmd.connectDestination(destContext)
	if err != nil {
		return fmt.Errorf("failed to connect to destination: %w", err)
	}

	return cmd.sendMessages(source, destination, srcContext, destContext, destTopic, dryRun, markReplayed)
}

func (cmd *command) connectSource(ctx context.Context, topic string, partitionIDs []int32, fromOffset, untilOffset, offsetsFile string, offsets []int64, fetchSize int32) (<-chan *sarama.ConsumerMessage, error) {
	client, err := cmd.newSourceClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	partitionRanges, partitionOffsets, err := cmd.parseSourceConfig(client, topic, partitionIDs, offsets, fromOffset, untilOffset, offsetsFile)
	if err != nil {
		return nil, err
	}

	if len(partitionRanges) > 0 && len(partitionOffsets) > 0 {
		return nil, fmt.Errorf("you cannot replay a range of offsets and individual offsets at the same time")
	}

	if len(partitionOffsets) > 0 {
		return cmd.consumeOffsets(ctx, client, topic, partitionOffsets, fetchSize)
	}

	return cmd.consumeRange(ctx, client, topic, partitionRanges)
}

func (cmd *command) newSourceClient() (sarama.Client, error) {
	conf := cmd.SaramaConfig()
	conf.Consumer.Return.Errors = false // TODO: log consumer errors

	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (cmd *command) parseSourceConfig(client sarama.Client, topic string, partitions []int32, offsets []int64, fromOffset, untilOffset, offsetsFile string) ([]internal.PartitionOffsetRange, []internal.PartitionOffsets, error) {
	if offsetsFile != "" {
		return cmd.parseOffsetsFile(client, offsetsFile, topic)
	}

	if len(partitions) == 0 {
		var err error
		partitions, err = client.Partitions(topic)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to determine amount of partitions: %w", err)
		}
	}

	if len(offsets) > 0 {
		if len(partitions) != 1 {
			return nil, nil, fmt.Errorf("cannot use --offset when consuming multiple partitions.\nPlease specify one partition using --partition or use --offsets-file to configure each partition offset separately")
		}

		return nil, []internal.PartitionOffsets{{Partition: partitions[0], Offsets: offsets}}, nil
	}

	startOffset, err := parseFromOffset(fromOffset, len(partitions))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid --from value")
	}

	maxOffsets, err := parseUntilOffset(untilOffset, client, topic, partitions)
	if err != nil {
		return nil, nil, err
	}

	var partitionRanges []internal.PartitionOffsetRange
	for _, partition := range partitions {
		if maxOffsets[partition] == -1 {
			// We want to read up until the most recent message but this partition
			// has never received any. Therefore, we don't need to process anything.
			cmd.logger.Printf("Ignoring topic %q partition %d because it contains no messages", topic, partition)
			continue
		}

		r := internal.PartitionOffsetRange{
			Partition: partition,
			From:      startOffset,
		}

		if untilOffset != "" {
			maxOffset := maxOffsets[partition]
			r.Until = &maxOffset
			cmd.logger.Printf("Consuming topic %q partition %d starting at offset %q until offset %d", topic, partition, r.From, r.Until)
		} else {
			cmd.logger.Printf("Consuming topic %q partition %d starting at offset %q indefinitely", topic, partition, r.From)
		}

		partitionRanges = append(partitionRanges, r)
	}

	return partitionRanges, nil, nil
}

func (cmd *command) parseOffsetsFile(client sarama.Client, path, topic string) ([]internal.PartitionOffsetRange, []internal.PartitionOffsets, error) {
	type OffsetsFile struct {
		Partitions []struct {
			Partition int32
			From, To  string
			Offsets   []int64
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}

	var file OffsetsFile
	err = json.NewDecoder(f).Decode(&file)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode file: %w", err)
	}

	var (
		partitionRanges  []internal.PartitionOffsetRange
		partitionOffsets []internal.PartitionOffsets
	)

	for _, p := range file.Partitions {
		if len(p.Offsets) > 0 {
			partitionOffsets = append(partitionOffsets, internal.PartitionOffsets{
				Partition: p.Partition,
				Offsets:   p.Offsets,
			})
		} else {
			r := internal.PartitionOffsetRange{Partition: p.Partition}

			switch p.From {
			case "", "oldest":
				r.From = sarama.OffsetOldest
			case "newest":
				r.From = sarama.OffsetNewest
			default:
				r.From, err = strconv.ParseInt(p.From, 10, 0)
				if err != nil {
					return nil, nil, fmt.Errorf(`bad "from" value for partition %d`, p.Partition)
				}
			}

			switch p.To {
			case "", "newest":
				maxOffset, err := client.GetOffset(topic, p.Partition, sarama.OffsetNewest)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to determine the newest offset value of partition %d: %w", p.Partition, err)
				}

				maxOffset = maxOffset - 1 // GetOffset returns the most recent *available* offset, not the last one
				r.Until = &maxOffset

			default:
				maxOffset, err := strconv.ParseInt(p.To, 10, 0)
				if err != nil {
					return nil, nil, fmt.Errorf(`bad "to" value for partition %d`, p.Partition)
				}

				r.Until = &maxOffset
			}

			partitionRanges = append(partitionRanges, r)
		}
	}

	return partitionRanges, partitionOffsets, nil
}

func parseFromOffset(offset string, numPartitions int) (int64, error) {
	switch offset {
	case "oldest":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:
		if numPartitions != 1 {
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

func (cmd *command) consumeOffsets(ctx context.Context, client sarama.Client, topic string, partitions []internal.PartitionOffsets, fetchSize int32) (<-chan *sarama.ConsumerMessage, error) {
	consumer := internal.NewOffsetConsumer(client, fetchSize, cmd.logger, cmd.debug)
	return consumer.Consume(ctx, topic, partitions)
}

func (cmd *command) consumeRange(ctx context.Context, client sarama.Client, topic string, partitions []internal.PartitionOffsetRange) (<-chan *sarama.ConsumerMessage, error) {
	consumer := internal.NewRangeConsumer(client, cmd.debug)
	return consumer.Consume(ctx, topic, partitions)
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
	source <-chan *sarama.ConsumerMessage,
	destination sarama.SyncProducer,
	srcContext, destContext, destTopic string,
	dryRun, markReplayed bool,
) error {
	var numErrors int
	for in := range source {
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

		outPartition, outOffset, err := destination.SendMessage(out)
		if err != nil {
			numErrors++
			cmd.logger.Printf("Error: failed to send message to destination topic: %v", err)
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

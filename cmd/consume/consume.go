package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *command) ConsumeCmd() *cobra.Command {
	produceCmd := &cobra.Command{
		Use:     "consume <topic>",
		GroupID: "consumer-producer",
		Args:    cobra.ExactArgs(1),
		Short:   "Consume messages from a Kafka topic and print them to stdout",
		Long: `Consume messages from a Kafka topic and print them to stdout.

By default, the --output flag is set to "json" which means that the command will
try to decode values and print their JSON representation to stdout, followed by
a newline. You can set --output=raw if you do not want to decode the data but
see the raw message as it is stored in Kafka.

You can set --output-meta to enable printing more information about each consumed
message, such as the message key as well as headers, partition and offset
information.

Values will automatically be decoded using the topic schema configuration from the
kafkactl configuration file (e.g. to decode proto messages and print them as JSON).
If no configuration matches the topic name, message values will be assumed to be
unicode strings.

This command will block as long as it is connected to Kafka. You can stop reading
messages by sending SIGINT, SIGQUIT or SIGTERM to the process (e.g. by pressing ctrl+c).
`,
		Example: `
    # Read and print all messages from "example-topic" without joining a consumer group
    kafkactl consume example-topic
    
    # Read messages only from a specific partition
    kafkactl consume example-topic --partition=1
    
    # Join the "test" consumer group and print all messages that are assigned to this member
    kafkactl consume example-topic --group=test
`,
		RunE: func(cc *cobra.Command, args []string) error {
			ctx := cli.Context()
			topic := args[0]
			partitions := viper.GetIntSlice("partition")
			offsetStr := viper.GetString("offset")
			groupID := viper.GetString("group")
			outputEncoding := viper.GetString("output")
			outputMeta := viper.GetBool("output-meta")

			if groupID != "" && cc.Flag("offset").Changed {
				return fmt.Errorf("cannot use --group and --offset together: the consumer group will determine the offset automatically")
			}
			if groupID != "" && cc.Flag("partition").Changed {
				return fmt.Errorf("cannot use --group and --partition together: the consumer group will determine the partition automatically")
			}
			if outputMeta && outputEncoding == "raw" {
				return fmt.Errorf("using --output-meta and --output=raw at the same time is currently not supported")
			}

			var offset int64
			switch offsetStr {
			case "oldest", "first":
				offset = sarama.OffsetOldest
			case "newest", "last":
				offset = sarama.OffsetNewest
			default:
				n, err := strconv.Atoi(offsetStr)
				if err != nil {
					return fmt.Errorf("failed to parse --offset as integer: %w", err)
				}
				offset = int64(n)
			}

			return cmd.consume(ctx, topic, partitions, offset, groupID, outputEncoding, outputMeta)
		},
	}

	flags := produceCmd.Flags()
	flags.IntSlice("partition", nil, "Kafka topic partition. Can be passed multiple times. By default all partitions are consumed")
	flags.String("offset", "newest", `either "oldest" or "newest". Can be an integer when consuming only a single partition`)
	flags.String("group", "", `join a consumer group. Cannot be used together with partition or offset fag`)
	flags.StringP("output", "o", "json", "output format. One of json|raw")
	flags.Bool("output-meta", false, "also print meta information about all consumed messages. See --help output for more information")

	return produceCmd
}

func (cmd *command) consume(ctx context.Context, topic string, partitions []int, offset int64, groupID, outputEncoding string, outputMeta bool) error {
	conf := cmd.Configuration()
	dec, err := internal.NewTopicDecoder(topic, *conf)
	if err != nil {
		return err
	}

	var messages <-chan *sarama.ConsumerMessage
	if groupID != "" {
		messages, err = cmd.joinConsumerGroup(ctx, topic, groupID)
		if err != nil {
			return err
		}
	} else {
		messages, err = cmd.simpleConsumer(ctx, topic, partitions, offset)
		if err != nil {
			return err
		}
	}

	for msg := range messages {
		if outputEncoding == "raw" {
			_, err := os.Stdout.Write(msg.Value)
			return err
		}

		decoded, err := dec.Decode(msg)
		if err != nil {
			return fmt.Errorf("failed to decode message from Kafka: %w", err)
		}

		var out = decoded.Value
		if outputMeta {
			out = decoded
		}

		jsonOutput, err := json.Marshal(out)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintln(os.Stdout, string(jsonOutput))
		if err != nil {
			return err
		}
	}

	return nil
}

func (cmd *command) joinConsumerGroup(ctx context.Context, topic, groupID string) (<-chan *sarama.ConsumerMessage, error) {
	saramaConf, err := cmd.SaramaConfig()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	saramaConf.Consumer.Return.Errors = false // TODO

	client, err := cmd.ConnectClient(saramaConf)
	if err != nil {
		return nil, err
	}

	cmd.logger.Printf("Consuming messages from topic %q via consumer group %q",
		topic, groupID,
	)

	return internal.JoinConsumerGroup(ctx, client, topic, groupID, cmd.logger)
}

func (cmd *command) simpleConsumer(ctx context.Context, topic string, partitions []int, offset int64) (<-chan *sarama.ConsumerMessage, error) {
	saramaConf, err := cmd.SaramaConfig()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	saramaConf.Consumer.Return.Errors = false // TODO

	client, err := cmd.ConnectClient(saramaConf)
	if err != nil {
		return nil, err
	}
	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	con := internal.NewConsumer(c)
	cmd.logger.Printf("Consuming messages from %s of topic %q starting at %s",
		humanFriendlyPartitions(partitions), topic, humanFriendlyOffset(offset),
	)

	switch len(partitions) {
	case 0:
		return con.ConsumeAllPartitions(ctx, topic, offset)
	case 1:
		return con.ConsumePartition(ctx, topic, int32(partitions[0]), offset)
	default:
		pp := make([]internal.PartitionOffset, len(partitions))
		for i, p := range partitions {
			pp[i] = internal.PartitionOffset{
				Partition: int32(p),
				Offset:    offset,
			}
		}
		return con.ConsumePartitions(ctx, topic, pp)
	}
}

func humanFriendlyPartitions(partitions []int) string {
	switch len(partitions) {
	case 0:
		return "all partitions"
	case 1:
		return fmt.Sprintf("partition %d", partitions[0])
	default:
		descr := "partitions"
		for i, p := range partitions {
			switch i {
			case 0:
				descr += " " + fmt.Sprint(p)
			case len(partitions) - 1:
				descr += " & " + fmt.Sprint(p)
			default:
				descr += ", " + fmt.Sprint(p)
			}
		}
		return descr
	}
}

func humanFriendlyOffset(offset int64) string {
	switch offset {
	case sarama.OffsetNewest:
		return "newest offset"
	case sarama.OffsetOldest:
		return "oldest offset"
	default:
		return fmt.Sprint("offset", offset)
	}
}

package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/pkg"
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

By default, the --output flag is set to "raw" which means that the command will
only print the message values followed by a newline. You can set --output=json
in order to print each consumed message as a JSON object which will contain the 
partition and offset information in addition to the message value.

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
		RunE: func(_ *cobra.Command, args []string) error {
			ctx := cli.Context()
			topic := args[0]
			partitions := viper.GetIntSlice("partition")
			offset := viper.GetString("offset")
			outputEncoding := viper.GetString("output")
			return cmd.consume(ctx, topic, partitions, offset, outputEncoding)
		},
	}

	flags := produceCmd.Flags()
	flags.IntSlice("partition", nil, "Kafka topic partition. Can be passed multiple times. By default all partitions are consumed")
	flags.String("offset", "newest", `either "oldest" or "newest". Can be an integer when consuming only a single partition`)
	flags.StringP("output", "o", "raw", "output format. One of raw|json. See --help output for more information")
	// TODO: support joining a consumer group

	return produceCmd
}

func (cmd *command) consume(ctx context.Context, topic string, partitions []int, offsetStr, outputEncoding string) error {
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

	conf := cmd.Configuration()
	dec, err := pkg.NewTopicDecoder(topic, *conf)
	if err != nil {
		return err
	}

	messages, err := cmd.simpleConsumer(ctx, topic, partitions, offset)
	if err != nil {
		return err
	}

	for msg := range messages {
		decoded, err := dec.Decode(msg)
		if err != nil {
			return fmt.Errorf("failed to decode message from Kafka: %w", err)
		}

		switch outputEncoding {
		case "raw":
			fmt.Fprintln(os.Stdout, decoded.Value)
		case "json":
			val, err := json.Marshal(decoded)
			if err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, string(val))
		}

	}

	return nil
}

func (cmd *command) simpleConsumer(ctx context.Context, topic string, partitions []int, offset int64) (<-chan *sarama.ConsumerMessage, error) {
	saramaConf := cmd.SaramaConfig()
	saramaConf.Consumer.Return.Errors = false // TODO

	client, err := cmd.ConnectClient(saramaConf)
	if err != nil {
		return nil, err
	}
	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	con := pkg.NewConsumer(c)

	switch len(partitions) {
	case 0:
		return con.ConsumeAllPartitions(ctx, topic, offset)
	case 1:
		return con.ConsumePartition(ctx, topic, int32(partitions[0]), offset)
	default:
		pp := make([]pkg.PartitionOffset, len(partitions))
		for i, p := range partitions {
			pp[i] = pkg.PartitionOffset{
				Partition: int32(p),
				Offset:    offset,
			}
		}
		return con.ConsumePartitions(ctx, topic, pp)
	}
}

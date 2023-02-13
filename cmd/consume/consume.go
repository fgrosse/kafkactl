package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *command) ConsumeCmd() *cobra.Command {
	produceCmd := &cobra.Command{
		Use:   "consume <topic>",
		Args:  cobra.ExactArgs(1),
		Short: "Consume messages from a Kafka topic and print them to stdout",
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
			partition := viper.GetInt32("partition")
			offset := viper.GetString("offset")
			outputEncoding := viper.GetString("output")
			return cmd.consume(ctx, topic, partition, offset, outputEncoding)
		},
	}

	flags := produceCmd.Flags()
	flags.Int32("partition", -1, "Kafka topic partition. -1 means all partitions")
	flags.String("offset", "newest", `either "oldest", "newest" or an integer`)
	flags.StringP("output", "o", "raw", "output format. One of raw|json. See --help output for more information")
	// TODO: support joining a consumer group

	return produceCmd
}

func (cmd *command) consume(ctx context.Context, topic string, partition int32, offsetStr, outputEncoding string) error {
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

	messages, err := cmd.simpleConsumer(ctx, topic, partition, offset)
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

func (cmd *command) simpleConsumer(ctx context.Context, topic string, partition int32, offset int64) (<-chan *sarama.ConsumerMessage, error) {
	conf := cmd.Configuration()
	saramaConf := cmd.SaramaConfig()
	saramaConf.Consumer.Return.Errors = false // TODO

	brokers := conf.Brokers(conf.CurrentContext)
	c, err := sarama.NewConsumer(brokers, saramaConf)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if partition >= 0 {
		return cmd.consumeSinglePartition(ctx, c, topic, partition, offset)
	}

	return cmd.consumeAllPartitions(ctx, c, topic, offset)
}

func (cmd *command) consumeSinglePartition(ctx context.Context, c sarama.Consumer, topic string, partition int32, offset int64) (<-chan *sarama.ConsumerMessage, error) {
	con, err := c.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, errors.Wrap(err, "failed to consume topic partition")
	}

	go func() {
		<-ctx.Done()
		con.AsyncClose()
	}()

	cmd.debug.Printf("Consuming topic %q partition %d starting at offset %d", topic, partition, offset)
	return con.Messages(), nil
}

func (cmd *command) consumeAllPartitions(ctx context.Context, c sarama.Consumer, topic string, offset int64) (<-chan *sarama.ConsumerMessage, error) {
	partitions, err := c.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "get partitions")
	}

	var wg sync.WaitGroup
	messages := make(chan *sarama.ConsumerMessage, len(partitions))

	for _, partition := range partitions {
		con, err := c.ConsumePartition(topic, partition, offset)
		if err != nil {
			return nil, errors.Wrapf(err, "consume partition %d", partition)
		}

		output := func(partitionMessages <-chan *sarama.ConsumerMessage) {
			defer wg.Done()
			for msg := range partitionMessages {
				select {
				case messages <- msg:
				case <-ctx.Done():
					return
				}
			}
		}

		wg.Add(1)
		go output(con.Messages())

		// Close this partition consumer when the context is done.
		go func() {
			<-ctx.Done()
			con.AsyncClose()
		}()
	}

	// When all individual partition consumers are done, close the messages channel.
	go func() {
		wg.Wait()
		close(messages)
	}()

	return messages, nil
}

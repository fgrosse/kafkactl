package get

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *command) GetMessageCmd() *cobra.Command {
	getMessageCmd := &cobra.Command{
		Use:   "message --topic=foo --offset=123",
		Args:  cobra.NoArgs,
		Short: "Consume messages from a Kafka cluster",
		Example: `
  # Print message with offset 81041238 from topic my-fancy-topic  
  kafkactl get message --topic=my-fancy-topic --offset=81041238
  
  # Read offsets from std in and print all corresponding messages
  kubectl logs -l app=my-app | jq 'select(…) | .offset' | kafkactl get message --offset=- --topic=my-fancy-topic`,
		RunE: func(_ *cobra.Command, args []string) error {
			ctx := cli.Context()
			offset := viper.GetString("offset")
			topic := viper.GetString("topic")
			partition := viper.GetInt32("partition")
			encoding := viper.GetString("output")
			return cmd.getMessage(ctx, offset, topic, partition, encoding)
		},
	}

	flags := getMessageCmd.Flags()
	flags.String("offset", "", `The Kafka offset that should be fetched. Can either be a number or the string "-" to read numbers from stdin (newline delimited)`)
	flags.String("topic", "", "Kafka topic")
	flags.Int32("partition", 0, "Kafka topic partition")

	// change default for --output flag
	flags.StringP("output", "o", "json", "Output format. One of json|raw|base64")

	_ = getMessageCmd.MarkFlagRequired("offset")
	_ = getMessageCmd.MarkFlagRequired("topic")

	return getMessageCmd
}

func (cmd *command) getMessage(ctx context.Context, offset, topic string, partition int32, encoding string) error {
	if encoding != "json" && encoding != "raw" && encoding != "base64" {
		return errors.New("only JSON and raw output are supported by this sub command")
	}

	if topic == "" {
		return errors.New("empty topic flag")
	}

	conf := cmd.Configuration()
	dec, err := internal.NewTopicDecoder(topic, *conf)
	if err != nil {
		return err
	}

	printMessage := func(offsetStr string) error {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			return fmt.Errorf("failed to parse offset from stdin: %w", err)
		}

		msg, err := cmd.fetchMessageForOffset(topic, partition, int64(offset))
		if err != nil {
			return fmt.Errorf("fetch message: %w", err)
		}

		switch encoding {
		case "raw":
			_, err := os.Stdout.Write(msg.Value)
			fmt.Fprintln(os.Stdout)
			if err != nil {
				return fmt.Errorf("write: %w", err)
			}
			return nil
		case "base64":
			_, err := base64.NewEncoder(base64.StdEncoding, os.Stdout).Write(msg.Value)
			fmt.Fprintln(os.Stdout)
			if err != nil {
				return fmt.Errorf("write: %w", err)
			}
			return nil
		}

		decoded, err := dec.Decode(msg)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}

		return cli.Print(encoding, decoded)
	}

	if offset == "-" {
		for line := range cli.ReadLines(ctx) {
			err := printMessage(line)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return printMessage(offset)
}

func (cmd *command) fetchMessageForOffset(topic string, partition int32, offset int64) (*sarama.ConsumerMessage, error) {
	conf, err := cmd.SaramaConfig()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	conf.Metadata.Full = false // we are only interested in very specific topics
	conf.Producer.Return.Successes = true

	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return nil, err
	}

	defer client.Close()

	broker, err := client.Leader(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to determine leader for partition %d of topic %q: %w", partition, topic, err)
	}

	return internal.FetchMessage(broker, topic, partition, offset, cmd.debug)
}

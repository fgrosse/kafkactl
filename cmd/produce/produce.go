package produce

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
)

func (cmd *command) ProduceCmd() *cobra.Command {
	produceCmd := &cobra.Command{
		Use:     "produce <topic>",
		GroupID: "consumer-producer",
		Short:   "Read messages from stdin and write them to a Kafka topic",
		Args:    cobra.ExactArgs(1),
		Example: `
  # Write each line entered into your terminal as new message to the Kafka topic "example-topic"
  kafkactl produce example-topic

  # Read newline delimited messages from a file and send them to the Kafka topic "example-topic"
  cat example-file | kafkactl produce example-topic
`,
		RunE: func(_ *cobra.Command, args []string) error {
			ctx := cli.Context()
			topic := args[0]
			return cmd.produce(ctx, topic)
		},
	}

	return produceCmd
}

func (cmd *command) produce(ctx context.Context, topic string) error {
	conf := cmd.Configuration()
	encoder, err := internal.NewTopicEncoder(topic, *conf)
	if err != nil {
		return fmt.Errorf("failed to create proto encoder: %w", err)
	}

	saramaConf, err := cmd.SaramaConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	c, err := cmd.ConnectClient(saramaConf)
	if err != nil {
		return err
	}

	defer c.Close()

	producer, err := sarama.NewAsyncProducerFromClient(c)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	var returnErr bool // whether any message failed to be sent to Kafka

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for msg := range producer.Successes() {
			cmd.debug.Printf("Successfully send message to Kafka partition=%d offset=%d", msg.Partition, msg.Offset)
		}
	}()

	go func() {
		defer wg.Done()
		for msg := range producer.Errors() {
			returnErr = true
			cmd.logger.Printf("Error: Failed to send message to Kafka: %v", msg.Err)
		}
	}()

	for line := range cli.ReadLines(ctx) {
		cmd.debug.Println("Sending next message")
		msg := &sarama.ProducerMessage{Topic: topic}
		msg.Value, err = encoder.Encode(line)
		if err != nil {
			returnErr = true
			cmd.logger.Printf("Error: failed to encode message: %v", err)
		}

		select {
		case <-ctx.Done():
			// outer loop will return
		case producer.Input() <- msg:
			// ok cool, next message please
		}
	}

	producer.AsyncClose()
	wg.Wait()

	if returnErr {
		return errors.New("at least one message failed")
	}

	return nil
}

package internal

import (
	"context"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type consumerGroupClient struct {
	messages chan *sarama.ConsumerMessage
	logger   *log.Logger
}

func JoinConsumerGroup(ctx context.Context, client sarama.Client, topic, groupID string, logger *log.Logger) (<-chan *sarama.ConsumerMessage, error) {
	c, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, fmt.Errorf("failed to creating consumer group client: %w", err)
	}

	messages := make(chan *sarama.ConsumerMessage)
	con := newConsumerGroupClient(messages, logger)

	go func() {
		defer close(messages)

		for {
			err := c.Consume(ctx, []string{topic}, con)

			switch {
			case ctx.Err() != nil:
				logger.Printf("\nConsumer was cancelled. Leaving consumer group")
				_ = c.Close()
				return
			case err != nil:
				logger.Printf("Error: failed to consume from Kafka: %s", err.Error())
			default:
				logger.Printf("Detected server-side consumer group rebalance. Acquiring new consumer group claims...")
			}
		}
	}()

	return messages, nil
}

func newConsumerGroupClient(messages chan *sarama.ConsumerMessage, logger *log.Logger) *consumerGroupClient {
	return &consumerGroupClient{
		messages: messages,
		logger:   logger,
	}
}

func (c *consumerGroupClient) Setup(sarama.ConsumerGroupSession) error { return nil }

func (c *consumerGroupClient) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *consumerGroupClient) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Printf("Consuming messages from partition %d of topic %q starting at %d",
		claim.Partition(), claim.Topic(), claim.InitialOffset(),
	)

	ctx := session.Context()
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil // messages channel was closed
			}

			c.messages <- msg
			session.MarkMessage(msg, "")

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

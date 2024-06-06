package internal

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/IBM/sarama"
)

type OffsetConsumer struct {
	logger    *log.Logger
	debug     *log.Logger
	client    sarama.Client
	fetchSize int32
}

type PartitionOffsets struct {
	Partition int32
	Offsets   []int64
}

func NewOffsetConsumer(client sarama.Client, fetchSize int32, logger, debug *log.Logger) *OffsetConsumer {
	return &OffsetConsumer{
		logger:    logger,
		debug:     debug,
		client:    client,
		fetchSize: fetchSize,
	}
}

func (c *OffsetConsumer) Consume(ctx context.Context, topic string, partitions []PartitionOffsets) (<-chan *sarama.ConsumerMessage, error) {
	leaders := map[int32]*sarama.Broker{}
	for _, p := range partitions {
		broker, err := c.client.Leader(topic, p.Partition)
		if err != nil {
			return nil, fmt.Errorf("failed to determine leader for partition %d of topic %q: %w", p.Partition, topic, err)
		}
		leaders[p.Partition] = broker
	}

	var wg sync.WaitGroup
	messages := make(chan *sarama.ConsumerMessage)
	for _, p := range partitions {
		wg.Add(1)

		go func(partition int32, offsets []int64) {
			leader := leaders[partition]
			c.consumePartition(ctx, leader, topic, partition, offsets, messages)
			wg.Done()
		}(p.Partition, p.Offsets)
	}

	go func() {
		wg.Wait()
		close(messages)
	}()

	return messages, nil
}

func (c *OffsetConsumer) consumePartition(
	ctx context.Context,
	broker *sarama.Broker,
	topic string,
	partition int32,
	offsets []int64,
	messages chan<- *sarama.ConsumerMessage,
) {
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	pending := map[int64]bool{}
	for _, o := range offsets {
		pending[o] = true
	}

	c.logger.Printf("Reading %d individual messages from topic %q partition %d, offsets: %v", len(offsets), topic, partition, offsets)

	for _, offset := range offsets {
		if !pending[offset] {
			// We already got this message with an earlier request (see comment below).
			continue
		}

		fetched, err := FetchMessages(broker, topic, partition, offset, c.fetchSize, c.debug)
		if err != nil {
			c.logger.Printf("ERROR: offset=%d: %v", offset, err)
			continue
		}

		// Due to the nature of Offset-fetches on Kafka it is possible to receive
		// more messages than what we asked for if they fit into the requested
		// fetch size. This is a server side optimization because Kafka assumes
		// we are processing messages sequentially.

		c.debug.Printf("Received %d messages for topic=%q partition=%d offset=%d", len(fetched), topic, partition, offset)

		if len(fetched) == 0 {
			c.logger.Printf("ERROR: Kafka returned no message for topic=%q partition=%d offset=%d", topic, partition, offset)
			continue
		}

		for _, msg := range fetched {
			if !pending[msg.Offset] {
				continue
			}

			delete(pending, msg.Offset)

			select {
			case <-ctx.Done():
				return
			case messages <- msg:
				// great, keep going!
			}
		}
	}
}

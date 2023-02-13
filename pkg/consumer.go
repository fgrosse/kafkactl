package pkg

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type Consumer struct {
	con sarama.Consumer
}

// PartitionOffset marks a specific offset in a Kafka topic partition.
type PartitionOffset struct {
	Partition int32
	Offset    int64 // can also be sarama.Newest or sarama.Oldest
}

func NewConsumer(con sarama.Consumer) *Consumer {
	return &Consumer{con: con}
}

func (c *Consumer) ConsumePartition(ctx context.Context, topic string, partition int32, offset int64) (<-chan *sarama.ConsumerMessage, error) {
	con, err := c.con.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, errors.Wrap(err, "failed to consume topic partition")
	}

	go func() {
		<-ctx.Done()
		con.AsyncClose()
	}()

	return con.Messages(), nil
}

func (c *Consumer) ConsumeAllPartitions(ctx context.Context, topic string, startOffset int64) (<-chan *sarama.ConsumerMessage, error) {
	if startOffset != sarama.OffsetOldest && startOffset != sarama.OffsetNewest {
		return nil, fmt.Errorf("startOffset must either be sarama.OffsetOldest or sarama.OffsetNewest")
	}

	partitions, err := c.con.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "get partitions")
	}

	startOffsets := make([]PartitionOffset, len(partitions))
	for i, p := range partitions {
		startOffsets[i] = PartitionOffset{
			Partition: p,
			Offset:    startOffset,
		}
	}

	return c.ConsumePartitions(ctx, topic, startOffsets)
}

func (c *Consumer) ConsumePartitions(ctx context.Context, topic string, partitions []PartitionOffset) (<-chan *sarama.ConsumerMessage, error) {
	var wg sync.WaitGroup
	messages := make(chan *sarama.ConsumerMessage, len(partitions))

	for _, partition := range partitions {
		con, err := c.con.ConsumePartition(topic, partition.Partition, partition.Offset)
		if err != nil {
			return nil, errors.Wrapf(err, "consume partition %d", partition)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range con.Messages() {
				messages <- msg
			}
		}()

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

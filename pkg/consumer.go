package pkg

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
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
		return nil, fmt.Errorf("failed to consume topic partition: %w", err)
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
		return nil, fmt.Errorf("failed to determine amount of partitions: %w", err)
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
	wg.Add(len(partitions))

	messages := make(chan *sarama.ConsumerMessage, len(partitions))
	consumePartition := func(con sarama.PartitionConsumer) {
		defer wg.Done()
		for msg := range con.Messages() {
			messages <- msg
		}
	}

	go func() {
		wg.Wait()
		close(messages)
	}()

	err := c.ProcessPartitions(ctx, topic, partitions, consumePartition)
	return messages, err
}

func (c *Consumer) ProcessPartitions(ctx context.Context, topic string, partitions []PartitionOffset, process func(sarama.PartitionConsumer)) error {
	consumers, err := c.connectPartitionConsumers(topic, partitions)
	if err != nil {
		return err
	}

	for _, con := range consumers {
		// Start processing this partition.
		go process(con)

		// Close this partition consumer when the context is done.
		go func(con sarama.PartitionConsumer) {
			<-ctx.Done()
			con.AsyncClose()
		}(con)
	}

	return nil
}

// connectPartitionConsumers sets up a sarama.PartitionConsumer for each PartitionOffset.
// Each partition consumer is connected concurrently which is a lot faster than
// doing this sequentially.
func (c *Consumer) connectPartitionConsumers(topic string, partitions []PartitionOffset) ([]sarama.PartitionConsumer, error) {
	type result struct {
		partition int32
		con       sarama.PartitionConsumer
		err       error
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	results := make(chan result)
	for _, p := range partitions {
		go func(p PartitionOffset) {
			con, err := c.con.ConsumePartition(topic, p.Partition, p.Offset)
			results <- result{
				partition: p.Partition,
				con:       con,
				err:       err,
			}
			wg.Done()
		}(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var consumers []sarama.PartitionConsumer
	var err error
	for r := range results {
		if r.err != nil {
			err = r.err
			continue
		}
		consumers = append(consumers, r.con)
	}

	if err != nil {
		for _, c := range consumers {
			_ = c.Close()
		}
		return nil, err
	}

	return consumers, nil
}

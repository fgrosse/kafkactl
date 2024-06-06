package internal

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type Consumer struct {
	con sarama.Consumer
}

// PartitionOffset marks a specific offset in a Kafka topic partition.
type PartitionOffset struct {
	Partition int32
	Offset    int64 // can also be sarama.Newest or sarama.Oldest
}

// Consume reads and returns messages from a Kafka topic using the provided configuration.
func Consume(ctx context.Context, client sarama.Client, topic string, partitions []int, offset int64, groupID string, logger *log.Logger) (<-chan *sarama.ConsumerMessage, error) {
	if groupID != "" {
		return JoinConsumerGroup(ctx, client, topic, groupID, logger)
	}

	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	con := NewConsumer(c)
	return con.Consume(ctx, topic, partitions, offset, logger)
}

func NewConsumer(con sarama.Consumer) *Consumer {
	return &Consumer{con: con}
}

func (c *Consumer) Consume(ctx context.Context, topic string, partitions []int, offset int64, logger *log.Logger) (<-chan *sarama.ConsumerMessage, error) {
	logger.Printf("Consuming messages from %s of topic %q starting at %s",
		humanFriendlyPartitions(partitions), topic, humanFriendlyOffset(offset),
	)

	switch len(partitions) {
	case 0:
		return c.ConsumeAllPartitions(ctx, topic, offset)
	case 1:
		return c.ConsumePartition(ctx, topic, int32(partitions[0]), offset)
	default:
		pp := make([]PartitionOffset, len(partitions))
		for i, p := range partitions {
			pp[i] = PartitionOffset{
				Partition: int32(p),
				Offset:    offset,
			}
		}
		return c.ConsumePartitions(ctx, topic, pp)
	}
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
		return fmt.Sprint("offset ", offset)
	}
}

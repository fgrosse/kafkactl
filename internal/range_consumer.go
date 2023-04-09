package internal

import (
	"context"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type RangeConsumer struct {
	logger *log.Logger
	client sarama.Client
}

type PartitionOffsetRange struct {
	Partition int32
	From      int64
	Until     *int64 // optional
}

func NewRangeConsumer(client sarama.Client, logger *log.Logger) *RangeConsumer {
	return &RangeConsumer{
		logger: logger,
		client: client,
	}
}

func (c *RangeConsumer) Consume(ctx context.Context, topic string, partitions []PartitionOffsetRange) (<-chan *sarama.ConsumerMessage, error) {
	partitionStarts := make([]PartitionOffset, len(partitions))
	maxOffsets := map[int32]int64{}
	for i, p := range partitions {
		partitionStarts[i] = PartitionOffset{
			Partition: p.Partition,
			Offset:    p.From,
		}

		if p.Until != nil {
			maxOffsets[p.Partition] = *p.Until
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	messages := make(chan *sarama.ConsumerMessage, len(partitions))
	consumePartition := func(con sarama.PartitionConsumer) {
		defer wg.Done()

		for msg := range con.Messages() {
			messages <- msg

			maxOffset, ok := maxOffsets[msg.Partition]
			if ok && msg.Offset == maxOffset {
				con.Close()
				c.logger.Printf("Partition consumer %d has reached its maximum offset", msg.Partition)
				return
			}
		}
	}

	go func() {
		wg.Wait()
		close(messages)
	}()

	con, err := sarama.NewConsumerFromClient(c.client)
	if err != nil {
		return nil, err
	}

	err = NewConsumer(con).ProcessPartitions(ctx, topic, partitionStarts, consumePartition)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

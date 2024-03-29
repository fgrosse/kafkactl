package internal

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

const (
	defaultFetchSizeBytes int32 = 1024 * 150       // 150KB
	maxFetchSizeBytes     int32 = 1024 * 1024 * 10 // 10MB
)

// FetchMessage returns a single message from Kafka. Note that if you want to
// retrieve more than a single message it might be more performant to use
// FetchMessages(…) instead.
func FetchMessage(broker *sarama.Broker, topic string, partition int32, offset int64, debugLogger *log.Logger) (*sarama.ConsumerMessage, error) {
	messages, err := FetchMessages(broker, topic, partition, offset, defaultFetchSizeBytes, debugLogger)
	if err != nil {
		return nil, err
	}

	for _, msg := range messages {
		if msg.Offset == offset {
			return msg, nil
		}
	}

	return nil, errors.New("not found")
}

// FetchMessages retrieves a bunch of messages starting at a given offset. This
// function is intended to be used if you are interested into a specific offset
// but for efficiency reasons, Kafka will actually return not only the requested
// message but also subsequent messages (i.e. ordered by offset) to fill up
// the response until the fetch size is reached. You can control how many bytes
// we are requesting using the fetchSizeBytes parameter.
func FetchMessages(broker *sarama.Broker, topic string, partition int32, offset int64, fetchSizeBytes int32, debugLogger *log.Logger) ([]*sarama.ConsumerMessage, error) {
	if fetchSizeBytes > maxFetchSizeBytes {
		return nil, errors.Errorf("message size is too big (%d bytes)", fetchSizeBytes)
	}

	debugLogger.Printf("Fetching offset=%v fetch-size=%d", offset, fetchSizeBytes)

	req := fetchOffsetRequest(topic, partition, offset, fetchSizeBytes)
	resp, err := broker.Fetch(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch messages from Kafka")
	}

	block := resp.GetBlock(topic, partition)
	if block == nil {
		return nil, errors.New("incomplete response from Kafka")
	}

	if block.Err != sarama.ErrNoError {
		return nil, errors.WithStack(block.Err)
	}

	// If the fetch size is too small, Kafka may return a partial response
	isPartial := block.Partial

	var messages []*sarama.ConsumerMessage
	for _, records := range block.RecordsSet {
		if records.MsgSet != nil {
			// Old Kafka versions return messages in this field and newer versions use RecordBatch instead.
			messages = append(messages, parseMessages(topic, partition, records.MsgSet)...)
			isPartial = isPartial || records.MsgSet.PartialTrailingMessage
		}

		if records.RecordBatch != nil {
			messages = append(messages, parseRecords(topic, partition, records.RecordBatch)...)
			isPartial = isPartial || records.RecordBatch.PartialTrailingRecord
		}
	}

	if len(messages) == 0 && isPartial {
		if fetchSizeBytes == maxFetchSizeBytes {
			return nil, errors.Errorf("received partial message but fetch size is already at maximum")
		}

		fetchSizeBytes *= 2
		if fetchSizeBytes > maxFetchSizeBytes {
			fetchSizeBytes = maxFetchSizeBytes
		}

		debugLogger.Printf("Received partial response and trying again with bigger fetch size offset=%v new-fetch-size=%d", offset, fetchSizeBytes)
		return FetchMessages(broker, topic, partition, offset, fetchSizeBytes, debugLogger)
	}

	return messages, nil
}

// For efficiency reasons Kafka will actually return not only the requested
// message but also subsequent messages (i.e. ordered by offset) to fill up
// the response until the fetch size is reached. We assume here that we are
// fetching a single message.
func fetchOffsetRequest(topic string, partition int32, offset int64, fetchSizeBytes int32) *sarama.FetchRequest {
	req := &sarama.FetchRequest{
		// MaxWaitTime is the maximum amount of time in milliseconds to
		// block waiting if insufficient data is available at the time the
		// request is issued
		MaxWaitTime: int32(5 * time.Second / time.Millisecond),

		// MinBytes is the minimum number of bytes of messages that must be
		// available to give a response. If the client sets this to 0 the
		// server will always respond immediately, however if there is no
		// new data since their last request they will just get back empty
		// message sets. If this is set to 1, the server will respond as soon
		// as at least one partition has at least 1 byte of data or the
		// specified timeout occurs. By setting higher values in combination
		// with the timeout the consumer can tune for throughput and trade
		// a little additional latency for reading only large chunks of data
		// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k
		// would allow the server to wait up to 100ms to try to accumulate
		// 64k of data before responding).
		MinBytes: 1,

		MaxBytes: sarama.MaxResponseSize,

		// Version relates to the Kafka version of the server. Version 3
		// can be used when the Kafka version is >= v0.10.0.
		Version: 5,

		// Isolation is a feature of a newer Kafka version but I copied this
		// setting here from the sarama library just in case upgrade one day.
		Isolation: sarama.ReadUncommitted,
	}

	req.AddBlock(topic, partition, offset, fetchSizeBytes, -1)

	return req
}

func parseMessages(topic string, partition int32, msgSet *sarama.MessageSet) []*sarama.ConsumerMessage {
	var messages []*sarama.ConsumerMessage
	for _, msgBlock := range msgSet.Messages {
		for _, msg := range msgBlock.Messages() {
			offset := msg.Offset
			if msg.Msg.Version >= 1 {
				baseOffset := msgBlock.Offset - msgBlock.Messages()[len(msgBlock.Messages())-1].Offset
				offset += baseOffset
			}

			messages = append(messages, &sarama.ConsumerMessage{
				Topic:          topic,
				Partition:      partition,
				Key:            msg.Msg.Key,
				Value:          msg.Msg.Value,
				Offset:         offset,
				Timestamp:      msg.Msg.Timestamp,
				BlockTimestamp: msgBlock.Msg.Timestamp,
			})
		}
	}

	return messages
}

func parseRecords(topic string, partition int32, batch *sarama.RecordBatch) []*sarama.ConsumerMessage {
	var messages []*sarama.ConsumerMessage
	for _, rec := range batch.Records {
		messages = append(messages, &sarama.ConsumerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       rec.Key,
			Value:     rec.Value,
			Offset:    batch.FirstOffset + rec.OffsetDelta,
			Timestamp: batch.FirstTimestamp.Add(rec.TimestampDelta),
			Headers:   rec.Headers,
		})
	}

	return messages
}

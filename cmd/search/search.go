package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/fgrosse/kafkactl/internal"
)

func (cmd *command) SearchCmd() *cobra.Command {
	searchCmd := &cobra.Command{
		Use:   "search <topic> <string>",
		Short: "Search for a string in a topic and print matching messages to stdout",
		Args:  cobra.ExactArgs(2),
		Example: `
	# Search for messages in "example-topic" that contain the string "foo"
	kafkactl search example-topic foo
`,
		RunE: func(cc *cobra.Command, args []string) error {
			ctx := cli.Context()
			topic := args[0]
			search := []byte(args[1])

			partitions := viper.GetIntSlice("partition")
			offsetStr := viper.GetString("offset")
			sinceStr := viper.GetString("since")
			groupID := viper.GetString("group")

			if groupID != "" && cc.Flag("offset").Changed {
				return fmt.Errorf("cannot use --group and --offset together: the consumer group will determine the offset automatically")
			}
			if groupID != "" && cc.Flag("partition").Changed {
				return fmt.Errorf("cannot use --group and --partition together: the consumer group will determine the partition automatically")
			}

			var since time.Time
			if sinceStr != "" {
				var err error
				since, err = time.Parse(time.RFC3339, sinceStr)
				if err != nil {
					return fmt.Errorf("failed to parse --since as RFC3339 time: %w", err)
				}
			}

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

			return cmd.search(ctx, topic, search, partitions, offset, since, groupID)
		},
	}

	flags := searchCmd.Flags()
	flags.IntSlice("partition", nil, "Kafka topic partition. Can be passed multiple times. By default all partitions are consumed")
	flags.String("offset", "newest", `either "oldest" or "newest". Can be an integer when consuming only a single partition`)
	flags.String("since", "", ``)                                                                            // TODO
	flags.String("group", "", `join a consumer group. Cannot be used together with partition or offset fag`) // TODO: use a group by default?

	return searchCmd
}

func (cmd *command) search(ctx context.Context, topic string, search []byte, partitions []int, offset int64, since time.Time, groupID string) error {
	conf := cmd.Configuration()
	dec, err := internal.NewTopicDecoder(topic, *conf)
	if err != nil {
		return err
	}

	saramaConf, err := cmd.SaramaConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	saramaConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConf.Consumer.Return.Errors = false // TODO
	saramaConf.Consumer.Fetch.Max = sarama.MaxResponseSize

	client, err := cmd.ConnectClient(saramaConf)
	if err != nil {
		return err
	}

	if !since.IsZero() {
		offset, err = client.GetOffset(topic, int32(partitions[0]), since.Unix())
		if err != nil {
			return fmt.Errorf("failed to get offset for time %s: %w", since, err)
		}
	}

	maxOffsets, err := cmd.fetchMaxOffsets(client, topic, partitions)
	var numMessages int
	for _, max := range maxOffsets {
		numMessages += int(max - offset)
	}

	messages, err := internal.Consume(ctx, client, topic, partitions, offset, groupID, cmd.logger)
	if err != nil {
		return err
	}

	progress := pb.New(numMessages)
	progress.Output = os.Stderr
	progress.ShowSpeed = true
	progress.ShowCounters = true
	progress.ShowPercent = true
	progress.ShowBar = false
	progress.ShowElapsedTime = true
	progress.ShowTimeLeft = true
	progress.ShowFinalTime = true
	progress.Prefix("Searching...")
	progress.SetUnits(pb.U_NO)
	progress.Start()
	defer progress.Finish()

	for msg := range messages {
		// TODO: by default stop when we reach the newest offset (per partition)

		progress.Increment()

		if msg.Value == nil {
			continue
		}

		decoded, err := dec.Decode(msg)
		if err != nil {
			cmd.logger.Printf("ERROR: failed to decode message from partition %d at offset %d: %s", msg.Partition, msg.Offset, err)
			continue
		}

		jsonOutput, err := json.Marshal(decoded)
		if err != nil {
			return err
		}

		if !bytes.Contains(jsonOutput, search) {
			continue
		}

		_, err = fmt.Fprintln(os.Stdout, string(jsonOutput))
		if err != nil {
			return err
		}

		if msg.Offset >= maxOffsets[msg.Partition] {
			cmd.logger.Printf("Reached end of partition %d", msg.Partition)
		}
	}

	return nil
}

func (cmd *command) fetchMaxOffsets(client sarama.Client, topic string, partitions []int) (map[int32]int64, error) {
	maxOffsets := map[int32]int64{}
	for _, p := range partitions {
		maxOffset, err := client.GetOffset(topic, int32(p), sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("failed to get newest offset for partition %d: %w", p, err)
		}
		maxOffsets[int32(p)] = maxOffset
	}
	return maxOffsets, nil
}

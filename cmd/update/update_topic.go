package update

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/constraints"
)

func (cmd *command) UpdateTopicCmd() *cobra.Command {
	updateConfigCmd := &cobra.Command{
		Use:   "topic <name> [<key>=<value>]...",
		Args:  cobra.MinimumNArgs(2),
		Short: "Update the configuration of a topic",
		Long: `
Update the configuration of a topic by providing a list of key-value pairs.

Hint: You can use "kafkactl get config --topic <name>" to see the current
      configuration for a given topic. 
`,
		Example: `
  # Update the topic retention configuration
  kafkactl update topic my-topic retention.ms=12000`,
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			kvPairs, err := cmd.parseKeyValuePairs(args[1:])
			if err != nil {
				return err
			}

			validateOnly := viper.GetBool("validate-only")
			return cmd.updateTopic(name, kvPairs, validateOnly)
		},
	}

	flags := updateConfigCmd.Flags()
	flags.Bool("validate-only", false, "only validate configuration updates")

	return updateConfigCmd
}

func (*command) parseKeyValuePairs(args []string) ([][2]string, error) {
	kvPairs := make([][2]string, len(args))
	for i, kvPair := range args {
		fields := strings.SplitN(kvPair, "=", 2)
		if len(fields) != 2 {
			return nil, fmt.Errorf("missing value for key %q", kvPair)
		}

		kvPairs[i] = [2]string{fields[0], fields[1]}
	}
	return kvPairs, nil
}

func (cmd *command) updateTopic(name string, kvPairs [][2]string, validateOnly bool) error {
	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	entries := make(map[string]*string, len(kvPairs))
	for _, kvPair := range kvPairs {
		key, value := kvPair[0], kvPair[1]
		entries[key] = &value
	}

	for key, val := range entries {
		cmd.debug.Printf("%s=%s", key, *val)
	}

	err = admin.AlterConfig(sarama.TopicResource, name, entries, validateOnly)
	if err != nil {
		if validateOnly {
			return fmt.Errorf("validation error: %w", err)
		}
		return fmt.Errorf("failed to alter topic configuration: %w", err)
	}

	for _, key := range keys(entries) {
		val := *entries[key]
		cmd.logger.Printf("Updated %s: %s", key, val)
	}

	return nil
}

func keys[K constraints.Ordered, V any](m map[K]V) []K {
	kk := make([]K, 0, len(m))
	for k := range m {
		kk = append(kk, k)
	}

	sort.Slice(kk, func(i, j int) bool {
		return kk[i] < kk[j]
	})

	return kk
}

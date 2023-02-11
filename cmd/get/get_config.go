package get

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config contains information displayed by "kafkactl get config".
type Config struct {
	Name      string `table:"NAME"`
	Value     string `table:"VALUE"`
	Default   bool   `table:"DEFAULT"`
	ReadOnly  bool   `table:"READ_ONLY"`
	Sensitive bool   `table:"-"`
	Source    string `table:"-"`
}

func (cmd *command) GetConfigCmd() *cobra.Command {
	getConfigCmd := &cobra.Command{
		Use:   "config",
		Short: "Get Kafka broker or topic configuration",
		Long: `Get Kafka broker or topic configuration

This command returns the Kafka broker configuration from the cluster by default.
You can also request the configuration for a specific topic by using the --topic
flag.

By default the result will be printed as table where values are limited to 40 characters.
If you want to know the full table you should use --output=wide or --output=json.

By default the command will print all configuration values but you can pass
arguments to limit the output to specific configuration only.
`,
		Example: `
  # Get the broker configuration of the cluster
  kafkactl get config
  
  # Get all configuration for topic "foobar"
  kafkactl get config --topic=foobar
  
  # Check if topic auto creation is enabled in the cluster
  kafkactl get config auto.create.topics.enable`,
		RunE: func(_ *cobra.Command, args []string) error {
			topic := viper.GetString("topic")
			encoding := viper.GetString("output")
			return cmd.getConfig(encoding, topic, args)
		},
	}

	flags := getConfigCmd.Flags()
	flags.StringP("topic", "t", "", "topic name to fetch the configuration from")
	flags.StringP("output", "o", "table", "Output format. One of json|yaml|table|wide") // add "wide" option to --output flag

	return getConfigCmd
}

func (cmd *command) getConfig(encoding, topic string, args []string) error {
	filter := map[string]bool{}
	for _, arg := range args {
		filter[arg] = true
	}

	admin, err := cmd.ConnectAdmin()
	if err != nil {
		return err
	}

	defer admin.Close()

	var req sarama.ConfigResource
	if topic != "" {
		req = sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        topic,
			ConfigNames: args,
		}
	} else {
		type cAdmin interface {
			Controller() (*sarama.Broker, error)
		}

		c, err := admin.(cAdmin).Controller()
		if err != nil {
			return fmt.Errorf("failed to determine cluster controller: %w", err)
		}

		req = sarama.ConfigResource{
			Type:        sarama.ClusterResource,
			Name:        fmt.Sprint(c.ID()),
			ConfigNames: args,
		}
	}

	resp, err := admin.DescribeConfig(req)
	if err != nil {
		return err
	}

	output := make([]Config, 0, len(resp))
	for _, conf := range resp {
		if _, ok := filter[conf.Name]; len(filter) > 0 && !ok {
			continue
		}

		val := conf.Value
		if encoding == "table" && len(val) > 40 {
			val = val[:40] + " …"
		}

		output = append(output, Config{
			Name:      conf.Name,
			Value:     val,
			ReadOnly:  conf.ReadOnly,
			Sensitive: conf.Sensitive,
			Default:   conf.Default,
			Source:    conf.Source.String(),
		})
	}

	if encoding == "wide" {
		encoding = "table"
	}

	return cli.Print(encoding, output)
}

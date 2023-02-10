package get

import (
	"sort"

	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Broker contains information displayed by "kafkactl get brokers".
type Broker struct {
	ID   int32
	Addr string
	Rack string `json:",omitempty"`
}

func (cmd *command) GetBrokersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "brokers",
		Args:  cobra.ExactArgs(0),
		Short: "List all active Kafka brokers as retrieved from cluster metadata",
		RunE: func(*cobra.Command, []string) error {
			encoding := viper.GetString("output")
			return cmd.getBrokers(encoding)
		},
	}
}

func (cmd *command) getBrokers(encoding string) error {
	conf := cmd.SaramaConfig()
	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return err
	}

	defer client.Close()

	var brokers []Broker
	for _, b := range client.Brokers() {
		brokers = append(brokers, Broker{
			ID:   b.ID(),
			Addr: b.Addr(),
			Rack: b.Rack(),
		})
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	return cli.Print(encoding, brokers)
}

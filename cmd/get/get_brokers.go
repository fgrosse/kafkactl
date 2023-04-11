package get

import (
	"fmt"
	"sort"

	"github.com/fgrosse/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Broker contains information displayed by "kafkactl get brokers".
type Broker struct {
	ID      int32  `table:"ID"`
	Address string `table:"ADDRESS"`
	Role    string `table:"ROLE" json:",omitempty" yaml:",omitempty"`
	Rack    string `table:"-" json:",omitempty" yaml:",omitempty"`
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
	conf, err := cmd.SaramaConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	client, err := cmd.ConnectClient(conf)
	if err != nil {
		return err
	}

	defer client.Close()

	controller, err := client.Controller()
	if err != nil {
		return fmt.Errorf("failed to get cluster controller: %w", err)
	}

	var brokers []Broker
	for _, b := range client.Brokers() {
		isController := b.ID() == controller.ID()
		var role string
		if isController {
			role = "controller"
		}

		brokers = append(brokers, Broker{
			ID:      b.ID(),
			Address: b.Addr(),
			Rack:    b.Rack(),
			Role:    role,
		})
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	return cli.Print(encoding, brokers)
}

package cmd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Kafkactl struct {
	*cobra.Command
	logger *log.Logger
	debug  *log.Logger
	conf   Configuration
}

func New() *Kafkactl {
	defaultConfigPath := filepath.Join(os.ExpandEnv("$HOME"), ".config", "kafkactl2", "config.yml") // TODO: s/kafkactl2/kafkactl

	cmd := &Kafkactl{
		logger: log.New(os.Stderr, "", 0),
		debug:  log.New(io.Discard, "", 0),
		Command: &cobra.Command{
			Use:   "kafkactl",
			Short: "kafkactl is a command line tool to interact with an Apache Kafka cluster",
		},
	}
	cmd.SilenceErrors = true

	flags := cmd.PersistentFlags()
	flags.String("config", defaultConfigPath, "path to kafkactl config file")
	flags.String("context", "", `the name of the kafkactl context to use (defaults to "current_context" field from config file)`)
	flags.BoolP("verbose", "v", false, "enable verbose output")
	viper.BindPFlags(flags)

	cmd.AddCommand(cmd.ConfigCmd())
	cmd.AddCommand(cmd.ContextCmd())
	cmd.AddCommand(cmd.GetCmd())

	cmd.PersistentPreRunE = cmd.initConfig

	return cmd
}

func (cmd *Kafkactl) initConfig(cc *cobra.Command, args []string) error {
	viper.SetEnvPrefix("KAFKACTL")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if viper.GetBool("verbose") {
		sarama.Logger = log.New(os.Stdout, "[SARAMA] ", 0)
		cmd.debug.SetOutput(os.Stderr)
		cmd.debug.SetPrefix("[DEBUG] ")
	}

	err := cmd.loadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	viper.BindPFlags(cc.Flags())
	cc.SilenceUsage = true

	return nil
}

func (cmd *Kafkactl) requireConfiguredContext() error {
	if len(cmd.conf.Contexts) == 0 {
		return fmt.Errorf("this command requires at least one configured context. Please use kafkactl config --help to get started")
	}

	return nil
}

func (cmd *Kafkactl) connectClient() (sarama.Client, error) {
	conf := cmd.saramaConfig()
	return cmd.connectClientWithConfig(conf)
}

func (cmd *Kafkactl) connectClientWithConfig(conf *sarama.Config) (sarama.Client, error) {
	if err := cmd.requireConfiguredContext(); err != nil {
		return nil, err
	}

	brokers := cmd.conf.Brokers()

	if len(brokers) == 0 {
		return nil, errors.New("need at least one broker")
	}

	// TODO: close client connection when context is closed?

	cmd.debug.Printf("Establishing initial connection to %q", brokers)
	return sarama.NewClient(brokers, conf)
}

func (cmd *Kafkactl) connectAdmin() (sarama.ClusterAdmin, error) {
	if err := cmd.requireConfiguredContext(); err != nil {
		return nil, err
	}

	brokers := cmd.conf.Brokers()

	if len(brokers) == 0 {
		return nil, errors.New("need at least one broker")
	}

	// TODO: close client connection when context is closed?

	return sarama.NewClusterAdmin(brokers, cmd.saramaConfig())
}

func (*Kafkactl) saramaConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.ClientID = "kafkactl"

	conf.Net.DialTimeout = 30 * time.Second
	conf.Net.ReadTimeout = 120 * time.Second
	conf.Net.WriteTimeout = 125 * time.Second
	conf.Admin.Timeout = 30 * time.Second

	conf.Metadata.Full = true

	return conf
}

package cmd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/kafkactl/cmd/config"
	"github.com/fgrosse/kafkactl/cmd/consume"
	"github.com/fgrosse/kafkactl/cmd/context"
	"github.com/fgrosse/kafkactl/cmd/create"
	"github.com/fgrosse/kafkactl/cmd/delete"
	"github.com/fgrosse/kafkactl/cmd/get"
	"github.com/fgrosse/kafkactl/cmd/produce"
	"github.com/fgrosse/kafkactl/cmd/replay"
	"github.com/fgrosse/kafkactl/cmd/update"
	"github.com/fgrosse/kafkactl/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Kafkactl struct {
	*cobra.Command
	logger *log.Logger
	debug  *log.Logger
	conf   *pkg.Configuration
}

func New() *Kafkactl {
	defaultConfigPath := filepath.Join(os.ExpandEnv("$HOME"), ".config", "kafkactl2", "config.yml") // TODO: s/kafkactl2/kafkactl
	logger := log.New(os.Stderr, "", 0)
	debug := log.New(io.Discard, "", 0)

	cmd := &Kafkactl{
		conf:   pkg.NewConfiguration(),
		logger: logger,
		debug:  debug,
		Command: &cobra.Command{
			Use:  "kafkactl",
			Long: `kafkactl is a command line tool to interact with an Apache Kafka cluster.`,
		},
	}
	cmd.SilenceErrors = true

	flags := cmd.PersistentFlags()
	flags.String("config", defaultConfigPath, "path to kafkactl config file")
	flags.String("context", "", `the name of the kafkactl context to use (defaults to "current_context" field from config file)`)
	flags.BoolP("verbose", "v", false, "enable verbose output")
	viper.BindPFlags(flags)

	cmd.AddCommand(context.Command(cmd))
	cmd.AddCommand(get.Command(cmd, logger, debug))
	cmd.AddCommand(config.Command(cmd, logger, debug))
	cmd.AddCommand(create.Command(cmd, logger, debug))
	cmd.AddCommand(delete.Command(cmd, logger, debug))
	cmd.AddCommand(update.Command(cmd, logger, debug))
	cmd.AddCommand(produce.Command(cmd, logger, debug))
	cmd.AddCommand(consume.Command(cmd, logger, debug))
	cmd.AddCommand(replay.Command(cmd, logger, debug))

	cmd.PersistentPreRunE = cmd.initConfig

	cmd.AddGroup(&cobra.Group{ID: "config", Title: "Managing configuration"})
	cmd.AddGroup(&cobra.Group{ID: "resources", Title: "Resource operations"})
	cmd.AddGroup(&cobra.Group{ID: "consumer-producer", Title: "Consuming & Producing messages"})

	cmd.SetHelpCommand(&cobra.Command{Hidden: true})

	return cmd
}

func (cmd *Kafkactl) initConfig(cc *cobra.Command, args []string) error {
	viper.SetEnvPrefix("KAFKACTL")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if viper.GetBool("verbose") {
		sarama.Logger = log.New(os.Stderr, "[SARAMA] ", 0)
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

func (cmd *Kafkactl) loadConfiguration() error {
	path := cmd.configFilePath()
	f, err := os.Open(path)
	switch {
	case os.IsNotExist(err):
		cmd.debug.Printf("Did not find configuration file at %q", path)
		return nil
	case err != nil:
		return err
	default:
		cmd.debug.Printf("Loading configuration from %q", path)
	}

	defer f.Close()
	cmd.conf, err = pkg.LoadConfiguration(f)
	if err != nil {
		return err
	}

	if c := viper.GetString("context"); c != "" {
		cmd.conf.CurrentContext = c
	}

	return nil
}

func (cmd *Kafkactl) configFilePath() string {
	return viper.GetString("config")
}

func (cmd *Kafkactl) SaveConfiguration() error {
	path := cmd.configFilePath()
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("create configuration directory: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create configuration file: %w", err)
	}

	err = pkg.SaveConfiguration(f, cmd.conf)
	if err != nil {
		return err
	}

	return f.Close()
}

func (cmd *Kafkactl) requireConfiguredContext() error {
	if len(cmd.conf.Contexts) == 0 {
		return fmt.Errorf("this command requires at least one configured context. Please use kafkactl config --help to get started")
	}

	return nil
}

func (cmd *Kafkactl) ConnectClient(conf *sarama.Config) (sarama.Client, error) {
	if err := cmd.requireConfiguredContext(); err != nil {
		return nil, err
	}

	context := cmd.conf.CurrentContext
	brokers := cmd.conf.Brokers(context)

	if len(brokers) == 0 {
		return nil, errors.New("need at least one broker")
	}

	cmd.debug.Printf("Establishing initial connection to %q", brokers)
	return sarama.NewClient(brokers, conf)
}

func (cmd *Kafkactl) ConnectAdmin() (sarama.ClusterAdmin, error) {
	if err := cmd.requireConfiguredContext(); err != nil {
		return nil, err
	}

	context := cmd.conf.CurrentContext
	brokers := cmd.conf.Brokers(context)

	if len(brokers) == 0 {
		return nil, errors.New("need at least one broker")
	}

	return sarama.NewClusterAdmin(brokers, cmd.SaramaConfig())
}

func (cmd *Kafkactl) Configuration() *pkg.Configuration {
	return cmd.conf
}

func (*Kafkactl) SaramaConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.ClientID = "kafkactl"
	conf.Metadata.Retry.Max = 0 // fail fast

	return conf
}

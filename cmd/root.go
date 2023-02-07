package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Kafkactl struct {
	root   *cobra.Command
	logger *log.Logger
	debug  *log.Logger
	conf   Configuration
}

func New() *Kafkactl {
	defaultConfigPath := filepath.Join(os.ExpandEnv("$HOME"), ".config", "kafkactl2", "config.yml") // TODO: s/kafkactl2/kafkactl

	cmd := &Kafkactl{
		logger: log.New(os.Stderr, "", 0),
		debug:  log.New(io.Discard, "", 0),
		root: &cobra.Command{
			Use:   "kafkactl",
			Short: "kafkactl is a command line tool to interact with an Apache Kafka cluster",
		},
	}
	cmd.root.SilenceErrors = true

	flags := cmd.root.PersistentFlags()
	flags.String("config", defaultConfigPath, "path to kafkactl config file")
	flags.String("context", "", `the name of the kafkactl context to use (defaults to "current_context" field from config file)`)
	flags.BoolP("verbose", "v", false, "enable verbose output")
	viper.BindPFlags(flags)

	cmd.root.AddCommand(cmd.ConfigCmd())
	cmd.root.AddCommand(cmd.ContextCmd())

	cmd.root.PersistentPreRunE = cmd.initConfig

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

func (cmd *Kafkactl) Execute() error {
	return cmd.root.Execute()
}

func (cmd *Kafkactl) requireConfiguredContext(*cobra.Command, []string) error {
	if len(cmd.conf.Contexts) == 0 {
		return fmt.Errorf("this command requires at least one configured context. Please use kafkactl config --help to get started")
	}

	return nil
}

package config

import (
	"fmt"

	"github.com/fgrosse/kafkactl/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cmd *command) ConfigAddContextCmd() *cobra.Command {
	addContextCmd := &cobra.Command{
		Use:   "add <name>",
		Args:  cobra.ExactArgs(1),
		Short: "Add a new configuration context to your kafkactl config file",
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			brokers := viper.GetStringSlice("broker")
			rootCAFile := viper.GetString("root-ca-file")
			username := viper.GetString("username")
			password := viper.GetString("password")
			keyFile := viper.GetString("key-file")
			certFile := viper.GetString("cert-file")
			return cmd.addContext(name, brokers, rootCAFile, username, password, keyFile, certFile)
		},
	}

	flags := addContextCmd.Flags()
	flags.StringSliceP("broker", "b", nil, "Kafka Broker address (can be passed multiple times)")

	flags.String("root-ca-file", "", "Path to the root CA file used when authenticating with the cluster")
	flags.String("username", "", "Username for SASL authentication")
	flags.String("password", "", "Username for SASL authentication")
	flags.String("key-file", "", "Path to the X509 key file to authenticate via TLS using a client certificate")
	flags.String("cert-file", "", "Path to the X509 certificate file to authenticate via TLS using a client certificate")

	addContextCmd.MarkFlagRequired("broker")

	return addContextCmd
}

func (cmd *command) addContext(name string, brokers []string, rootCAFile, username, password, keyFile, certFile string) error {
	contextConf := internal.ContextConfiguration{
		Name:    name,
		Brokers: brokers,
		Auth: internal.AuthConfiguration{
			RootCAFile: rootCAFile,
		},
	}
	if username != "" {
		contextConf.Auth.Method = "sasl"
		contextConf.Auth.Username = username
		contextConf.Auth.Password = password
	}
	if keyFile != "" {
		if contextConf.Auth.Method != "" {
			return fmt.Errorf("cannot use both password authentication and client certificates at the same time")
		}
		contextConf.Auth.Method = "tls"
		contextConf.Auth.KeyFile = keyFile
		contextConf.Auth.CertificateFile = certFile
	}

	conf := cmd.Configuration()
	err := conf.AddContext(contextConf)
	if err != nil {
		return err
	}

	err = cmd.SaveConfiguration()
	if err != nil {
		return err
	}

	cmd.logger.Printf("Successfully added new configuration context %q", name)

	return nil
}

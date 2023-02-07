package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type Configuration struct {
	APIVersion string                 `yaml:"api_version"`
	Contexts   []ContextConfiguration `yaml:"contexts"`

	CurrentContext  string `yaml:"current_context"`
	PreviousContext string `yaml:"previous_context"`
}

type ContextConfiguration struct {
	Name    string
	Brokers []string
}

func NewConfiguration() Configuration {
	return Configuration{APIVersion: "v1"}
}

func DefaultConfiguration() Configuration {
	conf := NewConfiguration()
	err := conf.AddContext("default", "localhost:9092")
	if err != nil {
		panic(err) // cannot happen unless there is a bug in kafkactl
	}

	return conf
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
	cmd.conf, err = LoadConfiguration(f)
	if err != nil {
		return err
	}

	return nil
}

func (cmd *Kafkactl) configFilePath() string {
	return viper.GetString("config")
}

func LoadConfiguration(r io.Reader) (Configuration, error) {
	conf := Configuration{}
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	err := dec.Decode(&conf)
	if err != nil {
		return conf, err
	}

	return conf, nil
}

func (cmd *Kafkactl) saveConfiguration() error {
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

	err = SaveConfiguration(f, cmd.conf)
	if err != nil {
		return err
	}

	return f.Close()
}

func SaveConfiguration(w io.Writer, conf Configuration) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	err := enc.Encode(conf)
	if err != nil {
		return err
	}

	return enc.Close()
}

func (conf *Configuration) AddContext(name string, brokers ...string) error {
	_, err := conf.GetContext(name)
	if err == nil {
		return fmt.Errorf("there is already a context named %q", name)
	}

	conf.Contexts = append(conf.Contexts, ContextConfiguration{
		Name:    name,
		Brokers: brokers,
	})

	if conf.CurrentContext == "" {
		conf.CurrentContext = name
	}

	return nil
}

func (conf *Configuration) GetContext(name string) (ContextConfiguration, error) {
	for _, c := range conf.Contexts {
		if c.Name == name {
			return c, nil
		}
	}

	return ContextConfiguration{}, fmt.Errorf("there is no context called %q", name)
}

func (conf *Configuration) SetContext(name string) error {
	if name == conf.CurrentContext {
		return nil
	}

	if name == "-" {
		return conf.toggleContext()
	}

	for _, c := range conf.Contexts {
		if c.Name == name {
			conf.PreviousContext = conf.CurrentContext
			conf.CurrentContext = c.Name
			return nil
		}
	}

	return fmt.Errorf("there is no context named %q", name)
}

func (conf *Configuration) toggleContext() error {
	if conf.PreviousContext != "" {
		conf.CurrentContext, conf.PreviousContext = conf.PreviousContext, conf.CurrentContext
		return nil
	}

	// If we don't have a previous context and we have more than two contexts
	// it is not clear what the user wants to do and we return an error
	if len(conf.Contexts) != 2 {
		return fmt.Errorf("cannot toggle context: no previous context was saved in configuration file")
	}

	for _, c := range conf.Contexts {
		if c.Name != conf.CurrentContext {
			conf.PreviousContext = c.Name
		}
	}

	conf.CurrentContext, conf.PreviousContext = conf.PreviousContext, conf.CurrentContext
	return nil
}

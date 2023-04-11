package internal

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Configuration struct {
	APIVersion      string `yaml:"api_version"`
	CurrentContext  string `yaml:"current_context"`
	PreviousContext string `yaml:"previous_context,omitempty"`

	Contexts []ContextConfiguration   `yaml:"contexts"`
	Topics   []*TopicConfig           `yaml:"topics,omitempty"`
	Proto    GlobalProtoDecoderConfig `yaml:"proto,omitempty"`
}

type ContextConfiguration struct {
	Name    string            `yaml:"name"`
	Brokers []string          `yaml:"brokers"`
	Auth    AuthConfiguration `yaml:"auth,omitempty"`

	SchemaRegistry SchemaRegistryConfiguration `yaml:"schema_registry,omitempty"`
}

type AuthConfiguration struct {
	Method     string `yaml:"method,omitempty"` // either tls or sasl
	RootCAFile string `yaml:"root_ca_file,omitempty"`

	Username string `yaml:"username,omitempty"` // only used when method=sasl
	Password string `yaml:"password,omitempty"` // only used when method=sasl

	KeyFile         string `yaml:"key_file,omitempty"`         // only used when method=tls
	CertificateFile string `yaml:"certificate_file,omitempty"` // only used when method=tls
}

type SchemaRegistryConfiguration struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

type TopicConfig struct {
	Name   string            `yaml:"name"`
	Schema TopicSchemaConfig `yaml:"schema"`
}

type TopicSchemaConfig struct {
	Type  string           `yaml:"type"` // "avro" or "proto"
	Proto TopicProtoConfig `yaml:"proto"`
}

type TopicProtoConfig struct {
	Type string `yaml:"type"`
	File string `yaml:"file"`
}

type TopicAvroConfig struct {
	RegistryURL     string `yaml:"registry_url"`
	PrintAvroSchema bool   `yaml:"print_avro_json"`
}

type GlobalProtoDecoderConfig struct {
	Includes []string `yaml:"includes,omitempty"`
}

func NewConfiguration() *Configuration {
	return &Configuration{APIVersion: "v1"}
}

func LoadConfiguration(r io.Reader) (*Configuration, error) {
	conf := new(Configuration)
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	err := dec.Decode(conf)
	if err != nil {
		return nil, err
	}

	return conf, nil
}

func SaveConfiguration(w io.Writer, conf *Configuration) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	err := enc.Encode(conf)
	if err != nil {
		return err
	}

	return enc.Close()
}

func (conf *Configuration) AddContext(name string, brokers ...string) error {
	_, err := conf.Context(name)
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

func (conf *Configuration) GetCurrentContext() (ContextConfiguration, error) {
	return conf.Context(conf.CurrentContext)
}

func (conf *Configuration) Context(name string) (ContextConfiguration, error) {
	if name == "" {
		return ContextConfiguration{}, errors.New("missing context name")
	}

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

func (conf *Configuration) DeleteContext(name string) error {
	var deleted bool
	for i, c := range conf.Contexts {
		if c.Name == name {
			switch i {
			case 0:
				conf.Contexts = conf.Contexts[i+1:]
			case len(conf.Contexts) - 1:
				conf.Contexts = conf.Contexts[:i]
			default:
				conf.Contexts = append(conf.Contexts[:i], conf.Contexts[i+1:]...)
			}
			deleted = true
			break
		}
	}

	if !deleted {
		return fmt.Errorf("there is no context called %q", name)
	}

	if name == conf.CurrentContext {
		conf.CurrentContext = conf.PreviousContext
		conf.PreviousContext = ""
	} else if name == conf.PreviousContext {
		conf.PreviousContext = ""
	}

	if len(conf.Contexts) == 1 {
		conf.CurrentContext = conf.Contexts[0].Name
	}

	// TODO: return an error if the current context is deleted and there is no previous context
	return nil
}

func (conf *Configuration) RenameContext(oldName, newName string) error {
	var ct *ContextConfiguration
	for i, c := range conf.Contexts {
		if c.Name == oldName {
			ct = &(conf.Contexts[i])
		}
		if c.Name == newName {
			return fmt.Errorf("there is already a context named %q", newName)
		}
	}

	if ct == nil {
		return fmt.Errorf("there is no context called %q", oldName)
	}

	ct.Name = newName // only rename when we know there is no conflict
	if conf.CurrentContext == oldName {
		conf.CurrentContext = newName
	}
	if conf.PreviousContext == oldName {
		conf.PreviousContext = newName
	}

	return nil
}

func (conf *Configuration) Brokers(context string) []string {
	ct, err := conf.Context(context)
	if err != nil {
		return nil
	}

	brokers := ct.Brokers
	for i, addr := range brokers {
		brokers[i] = ensurePort(addr)
	}

	return brokers
}

func (conf *Configuration) TopicConfig(topic string) (*TopicConfig, error) {
	for i, topicConf := range conf.Topics {
		re, err := regexp.Compile("^" + topicConf.Name + "$")
		if err != nil {
			return nil, errors.Errorf(`topic configuration "name" field of topic %d is no valid regular expression: %v`, i, err)
		}
		if re.MatchString(topic) {
			return topicConf, nil
		}
	}

	return nil, nil
}

func (conf *Configuration) SaramaConfig() (*sarama.Config, error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Version = sarama.V1_1_0_0
	saramaConf.ClientID = "kafkactl"
	saramaConf.Metadata.Retry.Max = 0 // fail fast

	err := conf.configureAuth(saramaConf)
	return saramaConf, err
}

func (conf *Configuration) configureAuth(saramaConf *sarama.Config) error {
	contextConf, err := conf.GetCurrentContext()
	if err != nil {
		return err
	}

	if contextConf.Auth.RootCAFile != "" {
		caCert, err := os.ReadFile(contextConf.Auth.RootCAFile)
		if err != nil {
			return fmt.Errorf("failed to read root CA file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		ok := caCertPool.AppendCertsFromPEM(caCert)
		if !ok {
			return fmt.Errorf("failed to parse root certificate")
		}

		saramaConf.Net.TLS.Enable = true
		saramaConf.Net.TLS.Config = &tls.Config{
			RootCAs: caCertPool,
		}
	}

	switch strings.ToLower(contextConf.Auth.Method) {
	case "sasl":
		if contextConf.Auth.Username == "" {
			return fmt.Errorf(`auth mode "sasl" requires a username to be configured`)
		}

		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		saramaConf.Net.SASL.User = contextConf.Auth.Username
		saramaConf.Net.SASL.Password = contextConf.Auth.Password

	case "tls":
		if contextConf.Auth.CertificateFile == "" || contextConf.Auth.KeyFile == "" {
			return fmt.Errorf(`auth mode "tls" requires a certifiacte and key to be configured`)
		}

		keypair, err := tls.LoadX509KeyPair(contextConf.Auth.CertificateFile, contextConf.Auth.KeyFile)
		if err != nil {
			return fmt.Errorf("failed to load client certificate and key file: %w", err)
		}

		if saramaConf.Net.TLS.Config == nil {
			saramaConf.Net.TLS.Config = new(tls.Config)
		}

		saramaConf.Net.TLS.Config.Certificates = []tls.Certificate{keypair}
	}

	return nil
}

func ensurePort(addr string) string {
	u, err := url.Parse("kafka://" + addr)
	if err == nil && u.Port() == "" {
		addr += ":9092"
	}
	return addr
}

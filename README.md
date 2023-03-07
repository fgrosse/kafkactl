<h1 align="center">Kafkactl</h1>
<p align="center">A command line tool to interact with an Apache Kafka cluster.</p>
<p align="center">
    <a href="https://github.com/fgrosse/kafkactl/releases"><img src="https://img.shields.io/github/tag/fgrosse/kafkactl.svg?label=version&color=brightgreen"></a>
    <a href="https://github.com/fgrosse/kafkactl/actions/workflows/test.yml"><img src="https://github.com/fgrosse/kafkactl/actions/workflows/test.yml/badge.svg"></a>
    <a href="https://github.com/fgrosse/kafkactl/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-BSD--3--Clause-blue.svg"></a>
</p>

---

`kafkactl` is a command line tool for people that work with [Apache Kafka][kafka].
It can be used to **query** information (e.g., brokers, topics, messages, consumers, etc.)
or **create**, **update**, and **delete** resources in the Kafka cluster. The command can
be used to **consume** and **produce** messages and has native support for Kafka messages
that are encoded as [Protocol Buffers][protobuf]. Finally, kafkactl implements more
advanced behaviour on top of these primitives, e.g. to **replay** messages on the same
or another cluster.

![](docs/demo.gif)

## Installation

You can either install a pre-compiled binary or compile from source.

### Pre-compiled binaries

Download the pre-compiled binaries from the [releases page][releases] and copy
them into your `$PATH`.

### Compiling from source

If you have [Go][go] installed, you can fetch the latest code and compile an
executable binary using the following command:

```
go get github.com/fgrosse/kafkactl
```

## Usage

`kafkactl` is intended to be used as CLI tool on your local machine. It is both
useful in day to day operations on the shell as well in automation and scripting.
The tool is split into multiple commands, each with its own help output. You can
see each commands usage by setting the `--help` flag.

```
$ kafkactl --help       
kafkactl is a command line tool to interact with an Apache Kafka cluster.

Usage:
  kafkactl [command]

Managing configuration
  config      Manage the kafkactl configuration
  context     Switch between different configuration contexts

Resource operations
  create      Create resources in the Kafka cluster
  delete      Delete resources in the Kafka cluster
  get         Display resources in the Kafka cluster
  update      Update resources in the Kafka cluster

Consuming & Producing messages
  consume     Consume messages from a Kafka topic and print them to stdout
  produce     Read messages from stdin and write them to a Kafka topic
  replay      Read messages from a Kafka topic and append them to the end of a topic

Additional Commands:
  completion  Generate the autocompletion script for the specified shell

Flags:
      --config string    path to kafkactl config file (default "/home/fgrosse/.config/kafkactl2/config.yml")
      --context string   the name of the kafkactl context to use (defaults to "current_context" field from config file)
  -h, --help             help for kafkactl
  -v, --verbose          enable verbose output

Use "kafkactl [command] --help" for more information about a command.
```

### Getting started

The first thing you need to do after installing `kafkactl` is to set up a
*configuration context*. Each context contains all information to connect to a
set of Kafka brokers which form a Kafka cluster.

```
kafkactl config add "my-context" --broker example.com:9092
```

This will add and activate a new configuration context named "my-context".
Each subsequent command that interacts with Kafka will be directed towards the
brokers of the currently active configuration context. This is similar to how
`kubectl` manages different contexts to talk to multiple Kubernetes clusters.

You can also *add*, *delete* and *rename* configuration contexts as well as *print*
the full kafkactl configuration using `kafkactl config` and its sub-commands.
If you want to learn more, try passing the `--help` flag. 

### Query information

Now you can run your first `kafkactl` command to query information from the cluster:

```
$ kafkactl get brokers                     
ID      ADDRESS      ROLE
1       kafka1:9092  controller  
2       kafka2:9093              
3       kafka3:9094

$ kafkactl get topics
NAME    PARTITIONS  REPLICATION  RETENTION
test-1  2           1            2 weeks   
test-2  4           3            7 days    
test-3  10          3            12 hours 
```

Apart from *brokers* and *topics*, you can also query information about the Kafka
cluster *configuration*, all known *consumer groups* and fetch individual *messages*
using `kafkactl get`.

### Output encoding

All sub commands of `kafkactl get` default to printing information in a human
friendly way (e.g. as a table). They also have an `--output` flag which allows for
other output encoding that is more suitable for scripting (e.g. JSON) and often
contains more information than what fits into the tabular format. 

### Writing to Kafka

You can use `kafkactl` to create and delete topics and update configuration.
Please refer to the corresponding `kafkactl --help` output for more information
and examples.

## Other kafkactls

There are multiple applications that call themselves `kafkactl`. All of them
have been developed independently but with similar feature sets.

This incarnation of `kafkactl` was created at [Fraugster][fraugster] in September 2017.
It was a useful tool for many years and we decided to keep it around even after Fraugster
ceased to exist, mainly because we are very used to it and maybe for sentimental reasons.

Other `kafkactl` implementations come with similar features (e.g. Protobuf support,
managing configuration with kubectl-like contexts). We list them here, so you can
pick the tool that serves your use case best:

* [`deviceinsight/kafkactl`](https://github.com/deviceinsight/kafkactl)
* [`jbvmio/kafkactl`](https://github.com/jbvmio/kafkactl)

## Built With

* [sarama](https://github.com/Shopify/sarama) - a Go library for Apache Kafka
* [cobra](https://github.com/spf13/cobra) - a library to build powerful CLI applications
* [viper](https://github.com/spf13/viper) - configuration with fangs
* [protoreflect](https://github.com/jhump/protoreflect) - reflection for Go Protocol Buffers
* [testify](https://github.com/stretchr/testify) - A simple unit test library
* _[and more][built-with]_

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of
conduct and on the process for submitting pull requests to this repository.

## Versioning

All significant (e.g. breaking) changes are documented in the [CHANGELOG.md](CHANGELOG.md).

After the v1.0 release we plan to use [SemVer](http://semver.org/) for versioning.
For the versions available, see the [releases page][releases].

## Authors

- **Friedrich Große** - *Initial work* - [fgrosse](https://github.com/fgrosse)
- Various folks at [Fraugster](https://github.com/fraugster)
  - **Julius Bachnick** - [juliusbachnick](https://github.com/juliusbachnick)
  - **Andreas Krennmair** - [akrennmair](https://github.com/akrennmair)
  - **Stefan Warman** - [warmans](https://github.com/warmans)
  - **Stefan Koshiw** - [panamafrancis](https://github.com/panamafrancis)
  - **Oleg Prozorov** - [oleg](https://github.com/oleg)
  - _and more..._

See also the list of [contributors][contributors] who participated in this project.

## License

This project is licensed under the BSD-3-Clause License - see the [LICENSE](LICENSE) file for details.

[kafka]: https://kafka.apache.org/
[protobuf]: https://protobuf.dev/
[go]: https://go.dev/
[releases]: https://github.com/fgrosse/kafkactl/releases
[fraugster]: https://github.com/fraugster
[contributors]: https://github.com/fgrosse/kafkactl/contributors
[built-with]: go.mod

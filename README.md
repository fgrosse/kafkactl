<h1 align="center">Kafkactl</h1>
<p align="center">A command line tool to interact with an Apache Kafka cluster.</p>

---

`kafkactl` is a command line tool for people that work with [Apache Kafka][kafka].
It can be used to **query** information (e.g., brokers, topics, messages, consumers, etc.)
or **create**, **update**, and **delete** resources in the Kafka cluster. The command can
be used to **consume** and **produce** messages and has native support for Kafka messages
that are encoded as [Protocol Buffers][protobuf]. Finally, kafkactl implements more
advanced behaviour on top of these primitives, e.g. to **replay** messages on the same
or another cluster.

## Installation

*TODO*

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

### Getting Started

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
test-1  1           1            7 days    
test-2  10          3            12 hours  
test-3  10          3            2 days  
```

Apart from *brokers* and *topics*, you can also query information about the Kafka
cluster *configuration*, all known *consumer groups* and fetch individual *messages*
using `kafkactl get`.

### Output encoding

All sub commands of `kafkactl get` default to printing information in a human
friendly way (e.g. as a table). They also have an `--output` flag which allows for
other output encoding that is more suitable for scripting and often contains more
information than what fits into the tabular format. 

## Roadmap

- [ ] Self update command
- [ ] Integration tests with Kafka
- [ ] Improve shell autocompletion

## Other kafkactls

* https://github.com/deviceinsight/kafkactl
* https://github.com/jbvmio/kafkactl

[kafka]: https://kafka.apache.org/
[protobuf]: https://protobuf.dev/

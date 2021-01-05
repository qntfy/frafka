# Frafka

[![Travis Build Status](https://img.shields.io/travis/qntfy/frafka.svg?branch=master)](https://travis-ci.org/qntfy/frafka)
[![Coverage Status](https://coveralls.io/repos/github/qntfy/frafka/badge.svg?branch=master)](https://coveralls.io/github/qntfy/frafka?branch=master)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![GitHub release](https://img.shields.io/github/release/qntfy/frafka.svg?maxAge=3600)](https://github.com/qntfy/frafka/releases/latest)
[![Go Report Card](https://goreportcard.com/badge/github.com/qntfy/frafka)](https://goreportcard.com/report/github.com/qntfy/frafka)
[![GoDoc](https://godoc.org/github.com/qntfy/frafka?status.svg)](http://godoc.org/github.com/qntfy/frafka)

Frafka is a Kafka implementation for [Frizzle](https://github.com/qntfy/frizzle) based on [confluent-go-kafka](https://github.com/confluentinc/confluent-kafka-go).

Frizzle is a magic message (`Msg`) bus designed for parallel processing w many goroutines.

* `Receive()` messages from a configured `Source`
* Do your processing, possibly `Send()` each `Msg` on to one or more `Sink` destinations
* `Ack()` (or `Fail()`) the `Msg`  to notify the `Source` that processing completed

## Prereqs / Build instructions

### Install librdkafka

The underlying kafka library,
[confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go#installing-librdkafka)
has some particularly important nuances:

* alpine builds (e.g. `FROM golang-1.14-alpine` should run all go commands with `-tags musl`
  * e.g. `go test -tags musl ./...`
* all builds producing an executable should run with `CGO_ENABLED=1`
  * not necessary for libraries, however.

Otherwise, should be good to go with

```sh
go get github.com/qntfy/frafka
cd frafka
go build
```

## Basic API usage

### Sink

Create a new sink with `NewSink`:

``` golang
// error omitted - handle in proper code
sink, _ := frafka.NewSink("broker1:15151,broker2:15151", 16 * 1024)
```

## Running the tests

Frafka has integration tests which require a kafka broker to test against. `KAFKA_BROKERS` environment variable is
used by tests. [simplesteph/kafka-stack-docker-compose](https://github.com/simplesteph/kafka-stack-docker-compose)
has a great simple docker-compose setup that is used in frafka CI currently.

```sh
curl --silent -L -o kafka.yml https://raw.githubusercontent.com/simplesteph/kafka-stack-docker-compose/v5.1.0/zk-single-kafka-single.yml
DOCKER_HOST_IP=127.0.0.1 docker-compose -f kafka.yml up -d
# takes a while to initialize; can use a tool like wait-for-it.sh in scripting
export KAFKA_BROKERS=127.0.0.1:9092
go test -v --cover ./...
```

## Configuration

Frafka Sources and Sinks are configured using [Viper](https://godoc.org/github.com/spf13/viper).

```golang
func InitSink(config *viper.Viper) (*Sink, error)

func InitSource(config *viper.Viper) (*Source, error)
```

We typically initialize Viper through environment variables (but client can do whatever it wants,
just needs to provide the configured Viper object with relevant values). The application might
use a prefix before the below values.

| Variable | Required | Description | Default |
|---------------------------|:--------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------:|
| KAFKA_BROKERS | required | address(es) of kafka brokers, space separated |  |
| KAFKA_TOPICS | source | topic(s) to read from |  |
| KAFKA_CONSUMER_GROUP | source | consumer group value for coordinating multiple clients |  |
| KAFKA_CONFIG | optional | Add librdkafka client config, format `key1=value1 key2=value2 ...` |  |
| KAFKA_CONFIG_FILE | optional | relative or absolute file path to a config file for librdkafka client config (see notes) |  |

### Kafka Client Configuration Notes

* `KAFKA_CONFIG` allows setting arbitrary
[librdkafka configuration](https://github.com/edenhill/librdkafka/blob/v1.4.2/CONFIGURATION.md)
such as `retries=10 max.in.flight=1000 delivery.report.only.error=true`
* `KAFKA_CONFIG_FILE` allows another method for arbitrary config similar to KAFKA_CONFIG. `KAFKA_CONFIG` takes priority
over `KAFKA_CONFIG_FILE`. The specified file is parsed with [viper](https://github.com/spf13/viper) which supports a range of
config file formats, for simplicity we recommend using yaml similar to the provided example file (used in tests).
* Required config set via environment variables listed above (e.g. `KAFKA_BROKERS`) will always take priority over
optional values - if `bootstrap.servers` is set in `KAFKA_CONFIG` to a different value, it will be ignored.
* Sensible defaults are set for several additional config values, see variables in `source.go` and `sink.go` for specifics
* An earlier version of frafka also supported setting specific optional kafka configs via environment variables, such as
compression. This functionality has been removed to simplify config logic and reduce confusion if values are set in multiple
places.

#### Suggested Kafka Config

Some values that we commonly set, particularly in a memory constrained environment (e.g. running a producer/consumer service against a 9 partition topic with average message size less than 10KB and less than 200MB memory available).

* queued.max.messages.kbytes: 2048 (up to 16384)
* auto.offset.reset: (latest|earliest)
* receive.message.max.bytes: 2000000
* fetch.max.bytes: 1000000
* compression.type: snappy (and possibly linger.ms value depending on throughput/latency requirements) are great to set to reduce network traffic and disk usage on brokers

## Async Error Handling

Since records are sent in batch fashion, Kafka may report errors or other information asynchronously.
Event can be recovered via channels returned by the `Sink.Events()` and `Source.Events()` methods.
Partition changes and EOF will be reported as non-error Events, other errors will conform to `error` interface.
Where possible, Events will retain underlying type from [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
if more information is desired.

## Contributing

Contributions welcome! Take a look at open issues.

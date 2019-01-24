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

### Go mod

As of Go 1.11, frafka uses [go mod](https://github.com/golang/go/wiki/Modules) for dependency management.

### Install librdkafka

Frafka depends on C library `librdkafka` (>=`v0.11.4`). For Debian 9+ (which includes golang docker images),
it has to be built from source. Fortunately, there's a script for that.
```
  # Install librdkafka
  - curl --silent -OL https://raw.githubusercontent.com/confluentinc/confluent-kafka-go/v0.11.4/mk/bootstrap-librdkafka.sh
  - bash bootstrap-librdkafka.sh v0.11.4 /usr/local
  - ldconfig
```

Once that is installed, should be good to go with
```
$ go get github.com/qntfy/frafka
$ cd frafka
$ go build
```

## Running the tests

Frafka has integration tests which require a kafka broker to test against. `KAFKA_BROKERS` environment variable is
used by tests. [simplesteph/kafka-stack-docker-compose](https://github.com/simplesteph/kafka-stack-docker-compose)
has a great simple docker-compose setup that is used in frafka CI currently.

```
$ curl --silent -L -o kafka.yml https://raw.githubusercontent.com/simplesteph/kafka-stack-docker-compose/v5.1.0/zk-single-kafka-single.yml
$ DOCKER_HOST_IP=127.0.0.1 docker-compose -f kafka.yml up -d
# takes a while to initialize; can use a tool like wait-for-it.sh in scripting
$ export KAFKA_BROKERS=127.0.0.1:9092
$ go test -v --cover ./...
```

## Configuration
Frafka Sources and Sinks are configured using [Viper](https://godoc.org/github.com/spf13/viper).
```
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
| KAFKA_CONSUME_LATEST_FIRST | source (optional) | start at the beginning or end of topic | earliest |

## Async Error Handling
Since records are sent in batch fashion, Kafka may report errors or other information asynchronously.
Event can be recovered via channels returned by the `Sink.Events()` and `Source.Events()` methods.
Partition changes and EOF will be reported as non-error Events, other errors will conform to `error` interface.
Where possible, Events will retain underlying type from [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
if more information is desired.

## Contributing
Contributions welcome! Take a look at open issues.

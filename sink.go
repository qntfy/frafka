package frafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/qntfy/frizzle"
	"github.com/spf13/viper"
)

var (
	_ frizzle.Sink    = (*Sink)(nil)
	_ frizzle.Eventer = (*Sink)(nil)
)

var (
	// how long to wait for messages to flush
	flushTimeoutMS = 10 * 1000

	defaultSinkKafkaCfg = &kafka.ConfigMap{
		"queued.max.messages.kbytes": 16384,
	}
)

// Sink encapsulates a kafka producer for Sending Msgs
type Sink struct {
	prod     *kafka.Producer
	quitChan chan struct{}
	doneChan chan struct{}
	evtChan  chan frizzle.Event
}

// NewSink sets up a basic frafka sink.
func NewSink(brokerString string, bufferSize int) (*Sink, error) {
	cfg := kafka.ConfigMap{
		"bootstrap.servers":          brokerString,
		"queued.max.messages.kbytes": bufferSize, // limit memory usage for the consumer prefetch buffer; note there is one buffer per topic+partition
	}
	return newSinkFromConfig(cfg)
}

// newSinkFromConfig sets up a frafka sink using a kafka.ConfigMap
func newSinkFromConfig(cfg kafka.ConfigMap) (*Sink, error) {
	p, err := kafka.NewProducer(&cfg)
	if err != nil {
		return nil, err
	}

	s := &Sink{
		prod:     p,
		quitChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		evtChan:  make(chan frizzle.Event),
	}

	go s.deliveryReports()

	return s, nil
}

// initSinkKafkaConfig does the heavy lifting for building out a kafka config for Sink Producer
// across possible configuration sources. It is extracted from InitSink for ease of unit testing.
func initSinkKafkaConfig(config *viper.Viper) (*kafka.ConfigMap, error) {
	if !config.IsSet("kafka_brokers") {
		return nil, errors.New("brokers must be set for kafka Sink")
	}

	kCfg, err := initBaseKafkaConfig(config, defaultSinkKafkaCfg)
	if err != nil {
		return nil, err
	}

	brokers := strings.Join(config.GetStringSlice("kafka_brokers"), ",")
	kCfg.SetKey("bootstrap.servers", brokers)

	return kCfg, nil
}

// InitSink initializes a basic Sink via *viper.Config.
func InitSink(config *viper.Viper) (*Sink, error) {
	kCfg, err := initSinkKafkaConfig(config)
	if err != nil {
		return nil, err
	}
	return newSinkFromConfig(*kCfg)
}

// deliveryReports receives async events from kafka Producer about whether
// message delivery is successful, any errors from broker, etc
func (s *Sink) deliveryReports() {
	defer close(s.doneChan)
	for {
		select {
		case <-s.quitChan:
			s.quitChan = nil
			return
		case e := <-s.prod.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					s.evtChan <- frizzle.NewError(m.TopicPartition.Error.Error())
				}
			case kafka.Error:
				s.evtChan <- frizzle.Event(e)
			default:
				s.evtChan <- frizzle.Event(e)
			}
		}
	}
}

// Events reports async Events that occur during processing
func (s *Sink) Events() <-chan frizzle.Event {
	return (<-chan frizzle.Event)(s.evtChan)
}

// Send a Msg to specified topic
func (s *Sink) Send(m frizzle.Msg, topic string) error {
	k := &kafka.Message{
		Value: m.Data(),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}
	s.prod.ProduceChannel() <- k
	return nil
}

// Close the Sink after flushing any Msgs not fully sent
func (s *Sink) Close() error {
	// check if already closed, return if so
	if s.quitChan == nil {
		return nil
	}

	// Flush any messages still pending send
	if remaining := s.prod.Flush(flushTimeoutMS); remaining > 0 {
		return fmt.Errorf("there are still %d messages which have not been delivered after %d milliseconds", remaining, flushTimeoutMS)
	}

	// tell deliveryReports() goroutine to finish if running
	close(s.quitChan)
	// wait for it to finish
	<-s.doneChan
	// stop event chan
	close(s.evtChan)
	s.prod.Close()
	return nil
}

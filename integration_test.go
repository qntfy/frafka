package frafka_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"github.com/qntfy/frafka"
	"github.com/qntfy/frizzle"
	"github.com/qntfy/frizzle/mocks"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

const (
	baseConsumerGroup = "testGroup"
	frizConsumerGroup = "frizGroup"
	testTopic         = "test.topic.1"
)

type sourceTestSuite struct {
	suite.Suite
	prod  *kafka.Producer
	admin *kafka.AdminClient
	v     *viper.Viper
	topic string
	src   frizzle.Source
}

type sinkTestSuite struct {
	suite.Suite
	cons  *kafka.Consumer
	v     *viper.Viper
	topic string
	sink  frizzle.Sink
}

func loadKafkaTestENV() string {
	// Setup viper and config from ENV
	v := viper.New()
	v.AutomaticEnv()
	v.SetDefault("kafka_brokers", "0.0.0.0:9092")
	return v.GetString("kafka_brokers")
}

func TestKafkaSource(t *testing.T) {
	suite.Run(t, new(sourceTestSuite))
}

func TestKafkaSink(t *testing.T) {
	suite.Run(t, new(sinkTestSuite))
}

func (s *sourceTestSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	brokers := loadKafkaTestENV()
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"session.timeout.ms": 6000,
	}
	var err error
	s.prod, err = kafka.NewProducer(cfg)
	if !s.Nil(err) {
		s.FailNow("unable to establish connection during test setup")
	}

	s.admin, err = kafka.NewAdminClient(cfg)
	if !s.Nil(err) {
		s.FailNow("unable to establish connection during test setup")
	}

	s.v = viper.New()
	s.v.Set("kafka_brokers", brokers)
	s.v.Set("kafka_consumer_group", frizConsumerGroup)
}

func (s *sourceTestSuite) TearDownSuite() {
	s.prod.Close()
	s.admin.Close()
}

func (s *sinkTestSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	brokers := loadKafkaTestENV()
	var err error
	s.cons, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          baseConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if !s.Nil(err) {
		s.FailNow("unable to establish connection during test setup")
	}

	s.v = viper.New()
	s.v.Set("kafka_brokers", brokers)
}

func (s *sinkTestSuite) TearDownSuite() {
	s.NoError(s.cons.Close())
}

func kafkaTopic(s string) string {
	r := strings.Replace(s, "/", ".", -1)
	suffix := strconv.Itoa(rand.Intn(100000))
	return strings.Join([]string{r, "topic", suffix}, ".")
}

func (s *sourceTestSuite) pingTopic(topic string) error {
	meta, err := s.admin.GetMetadata(&topic, false, 1000)
	if err != nil {
		return err
	} else if kafkaErr := meta.Topics[topic].Error; kafkaErr.Code() != kafka.ErrNoError {
		return errors.WithMessagef(kafkaErr, "topic %s has error", topic)
	} else if len(meta.Topics[topic].Partitions) < 1 {
		return errors.New(fmt.Sprintf("configured topic %s has no partitions", topic))
	}
	return nil
}

func (s *sourceTestSuite) SetupTest() {
	s.topic = kafkaTopic(s.T().Name())
	s.v.Set("kafka_topics", s.topic)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, err := s.admin.CreateTopics(ctx, []kafka.TopicSpecification{
		kafka.TopicSpecification{
			Topic:             s.topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	if !s.Nil(err) || !s.Equal(kafka.ErrNoError, results[0].Error.Code()) {
		s.FailNow("unable to create test topic")
	}

	// wait for topic to fully initialize
	var errTopic error
	for i := 0; i < 20; i++ {
		if errTopic = s.pingTopic(s.topic); errTopic == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if errTopic != nil {
		s.FailNow("topic did not initialize in 4+ seconds", errTopic.Error())
	}

	s.src, err = frafka.InitSource(s.v)
	if !s.Nil(err) {
		s.FailNow("unable to initialize source")
	}
	go func() {
		for ev := range s.src.(frizzle.Eventer).Events() {
			s.T().Logf("async message: %s", ev)
		}
	}()
}

func (s *sourceTestSuite) TearDownTest() {
	s.src = nil
}

func (s *sinkTestSuite) SetupTest() {
	s.topic = kafkaTopic(s.T().Name())
	err := s.cons.Subscribe(s.topic, nil)
	if !s.Nil(err) {
		s.FailNow("consumer unable to subscribe to topic")
	}

	s.sink, err = frafka.InitSink(s.v)
	if !s.Nil(err) {
		s.FailNow("unable to initialize sink")
	}

	go func() {
		for ev := range s.sink.(frizzle.Eventer).Events() {
			s.T().Logf("async message: %s", ev)
		}
	}()
}

func (s *sinkTestSuite) TearDownTest() {
	s.sink.Close()
	s.sink = nil
}

func (s *sinkTestSuite) TestSend() {
	expectedMessages := []string{"Hello", "out", "there", "kafka", "world!"}
	receivedMessages := []string{}
	s.T().Log(s.topic)

	for _, m := range expectedMessages {
		msg := frizzle.NewSimpleMsg("foo", []byte(m), time.Now())
		s.sink.Send(msg, s.topic)
	}

	for len(receivedMessages) < len(expectedMessages) {
		ev := s.cons.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			receivedMessages = append(receivedMessages, string(e.Value))
		case kafka.PartitionEOF:
			s.Failf("received PartitionEOF, try rerunning test", "%% Received: %v\n", e)
		case kafka.Error:
			s.FailNowf("received Kafka error", "%% Error: %v\n", e)
		}
	}

	s.Equal(expectedMessages, receivedMessages)
}

func (s *sinkTestSuite) TestCloseTwice() {
	err := s.sink.Close()
	s.Nil(err)
	err = s.sink.Close()
	s.Nil(err)
}

func (s *sourceTestSuite) produce(values []string) {
	for _, m := range values {
		s.prod.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &s.topic, Partition: kafka.PartitionAny},
			Value:          []byte(m),
		}, nil)
	}
}

func (s *sourceTestSuite) TestReceive() {
	expectedValues := []string{"now", "we", "receive", "some", "new", "messages"}
	receivedValues := []string{}
	s.T().Log(s.topic)

	s.produce(expectedValues)
	receivedMessages := []frizzle.Msg{}
	for len(receivedMessages) < len(expectedValues) {
		select {
		case m := <-s.src.Receive():
			receivedMessages = append(receivedMessages, m)
		}
	}

	// Confirm all show up in UnAcked()
	s.Equal(len(expectedValues), len(s.src.UnAcked()))

	for _, m := range receivedMessages {
		receivedValues = append(receivedValues, string(m.Data()))
		s.src.Ack(m)
	}

	// Confirm all have been Acked and received and expected values match
	s.Equal(0, len(s.src.UnAcked()))
	s.Equal(expectedValues, receivedValues)
}

func (s *sourceTestSuite) TestUnAckedAndFlush() {
	expectedValues := []string{"testing out", "stop", "and unacked"}
	receivedValues := []string{}
	s.T().Log(s.topic)

	s.produce(expectedValues)
	mSink := &mocks.Sink{}
	mSink.On("Close").Return(nil)
	f := frizzle.Init(s.src, mSink)

	for len(receivedValues) < len(expectedValues)-1 {
		select {
		case m := <-f.Receive():
			receivedValues = append(receivedValues, string(m.Data()))
			f.Ack(m)
		}
	}

	lastMsg := <-f.Receive()
	s.Equal("and unacked", string(lastMsg.Data()))
	s.Equal(1, len(s.src.UnAcked()))

	s.Nil(f.FlushAndClose(50 * time.Millisecond))
	s.Equal(0, len(s.src.UnAcked()))
	s.Equal(frizzle.ErrAlreadyAcked, f.Fail(lastMsg))

}

func (s *sourceTestSuite) TestPing() {
	kSrc := s.src.(*frafka.Source)
	err := kSrc.Ping()
	s.Nil(err, "expect successful Ping() for connected Source")

	// ensure it fails on invalid broker
	v := viper.New()
	v.Set("kafka_brokers", "abc.def.ghij:9092")
	v.Set("kafka_topics", s.topic)
	v.Set("kafka_consumer_group", frizConsumerGroup)

	_, err = frafka.InitSource(v)
	s.Error(err, "expected error from invalid broker")
}

func (s *sourceTestSuite) TestStopAndClose() {
	expectedValues := []string{"an unacked msg"}
	s.T().Log(s.topic)
	s.produce(expectedValues)

	lastMsg := <-s.src.Receive()
	s.Equal("kafka source: need to call Stop() before Close()", s.src.Close().Error())
	s.src.Stop()
	s.Equal(frizzle.ErrUnackedMsgsRemain, s.src.Close())
	s.src.Fail(lastMsg)
	s.Nil(s.src.Close())
}

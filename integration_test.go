package frafka_test

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	var err error
	s.prod, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		s.Error(err)
		s.FailNow("unable to establish connection during test setup")
	}

	s.v = viper.New()
	s.v.Set("kafka_brokers", brokers)
	s.v.Set("kafka_consumer_group", frizConsumerGroup)
}

func (s *sourceTestSuite) TearDownSuite() {
	s.prod.Close()
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
	if err != nil {
		s.Error(err)
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

func (s *sourceTestSuite) SetupTest() {
	s.topic = kafkaTopic(s.T().Name())
	s.v.Set("kafka_topics", s.topic)

	var err error
	s.src, err = frafka.InitSource(s.v)
	if err != nil {
		s.Error(err)
		s.Fail("unable to initialize source")
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
	if err != nil {
		s.Error(err)
		s.Fail("consumer unable to subscribe to topic")
	}

	s.sink, err = frafka.InitSink(s.v)
	if err != nil {
		s.Error(err)
		s.Fail("unable to initialize sink")
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

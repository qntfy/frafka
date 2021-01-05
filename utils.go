package frafka

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gofrs/uuid"
	"github.com/qntfy/frizzle"
	"github.com/spf13/viper"
)

// InitByViper initializes a full Frizzle with a kafka Source and Sink based on a provided Viper
func InitByViper(v *viper.Viper) (frizzle.Frizzle, error) {
	src, err := InitSource(v)
	if err != nil {
		return nil, err
	}
	sink, err := InitSink(v)
	if err != nil {
		return nil, err
	}
	return frizzle.Init(src, sink), nil
}

// generateID generates a unique ID for a Msg
func generateID() string {
	id, _ := uuid.NewV4()
	return id.String()
}

// initBaseKafkaConfig using common paths for both source and sink.
// Order of priority for kafka config:
// 1. Explicitly set keys specific to source / sink (set in their constructors)
// 2. arbitrary config set via environment variable from kafka_config
// 3. arbitrary config from kafka_config_file
// (We set the config in reverse order so the higher priority will override if set)
func initBaseKafkaConfig(v *viper.Viper, defaultKafkaCfg *kafka.ConfigMap) (*kafka.ConfigMap, error) {
	kCfg := kafka.ConfigMap{}

	if v.IsSet("kafka_config_file") {
		// set KeyDelimiter since the default is "." which is used in kafka config keys
		kafkaCfgFile := viper.NewWithOptions(viper.KeyDelimiter("|||"))
		kafkaCfgFile.SetConfigFile(v.GetString("kafka_config_file"))
		if err := kafkaCfgFile.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				return nil, errors.New("no file found at provided kafka_config_file path")
			}
			return nil, err
		}
		for k, v := range kafkaCfgFile.AllSettings() {
			kCfg.SetKey(k, kafka.ConfigValue(v))
		}
	}

	for _, c := range v.GetStringSlice("kafka_config") {
		kCfg.Set(c)
	}

	for k, v := range *defaultKafkaCfg {
		if existingVal, _ := kCfg.Get(k, nil); existingVal == nil {
			kCfg.SetKey(k, v)
		}
	}

	return &kCfg, nil
}

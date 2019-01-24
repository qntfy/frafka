package frafka

import (
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

package model

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

type MutateKafkaMessage struct {
	HBaseName  string
	MutateType int
	MutateData *Mutate
}

func GenerateKafkaMessage(topic string, mutate *Mutate, mutateType int, successName string) (*sarama.ProducerMessage, error) {
	msg := &MutateKafkaMessage{
		HBaseName:  successName,
		MutateType: mutateType,
		MutateData: mutate,
	}

	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}, nil
}

package kafka

import (
	"errors"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	kafkaFranzInputsLock sync.RWMutex
	kafkaFranzInputs     = map[string]kafkaFranzInput{}
)

type kafkaFranzInput struct {
	client *kgo.Client
	topics []string
}

func setKafkaFranzInput(label string, client *kgo.Client, topics []string) {
	kafkaFranzInputsLock.Lock()
	defer kafkaFranzInputsLock.Unlock()
	kafkaFranzInputs[label] = kafkaFranzInput{
		client: client,
		topics: topics,
	}
}

func getKafkaFranzInput(label string) (kafkaFranzInput, error) {
	kafkaFranzInputsLock.Lock()
	defer kafkaFranzInputsLock.Unlock()

	if input, ok := kafkaFranzInputs[label]; ok {
		return input, nil
	}

	return kafkaFranzInput{}, errors.New("input not found")
}

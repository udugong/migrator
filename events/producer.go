package events

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"

	"github.com/udugong/migrator"
)

type Producer[T migrator.IntegerOrString] interface {
	ProduceInconsistentEvent(ctx context.Context, event InconsistentEvent[T]) error
	ProduceInconsistentEvents(ctx context.Context, events []InconsistentEvent[T]) error
}

type KafkaGoProducer[T migrator.IntegerOrString] struct {
	writer *kafka.Writer
}

func NewKafkaGoProducer[T migrator.IntegerOrString](writer *kafka.Writer) *KafkaGoProducer[T] {
	return &KafkaGoProducer[T]{writer: writer}
}

func (k *KafkaGoProducer[T]) ProduceInconsistentEvent(ctx context.Context, event InconsistentEvent[T]) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	key, err := json.Marshal(event.UniqueKey)
	if err != nil {
		return err
	}
	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

func (k *KafkaGoProducer[T]) ProduceInconsistentEvents(ctx context.Context, events []InconsistentEvent[T]) error {
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			return err
		}
		key, err := json.Marshal(event.UniqueKey)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Key:   key,
			Value: value,
		})
	}
	return k.writer.WriteMessages(ctx, messages...)
}

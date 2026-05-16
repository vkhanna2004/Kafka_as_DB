package storage

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

func InitializeKafkaProducer() (*KafkaProducer, error) {
	p, error := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if error != nil {
		fmt.Printf("Failed to create producer: %s\n", error)
		return nil, error
	}
	return &KafkaProducer{producer: p}, nil
}

func (p *KafkaProducer) Set(topic, key, value string) error {

	deliveryChan := make(chan kafka.Event)

	err := p.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(key),
			Value: []byte(value),
		},
		deliveryChan,
	)

	if err != nil {
		return err
	}

	event := <-deliveryChan

	msg := event.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		return msg.TopicPartition.Error
	}

	fmt.Println("message delivered successfully")

	close(deliveryChan)

	return nil
}

type KafkaConsumer struct {
	consumer *kafka.Consumer
	store    *MemoryStore
}

func InitializeKafkaConsumer(m *MemoryStore) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "kvsdb-wal-group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		consumer: c,
		store:    m,
	}, nil
}

func (k *KafkaConsumer) Subscribe(topic string) error {
	return k.consumer.SubscribeTopics([]string{topic}, nil)
}

func (k *KafkaConsumer) Poll() {

	for {
		msg, err := k.consumer.ReadMessage(-1)

		if err != nil {
			fmt.Println("consumer error:", err)
			continue
		}
		k.store.Set(string(msg.Key), string(msg.Value))
		fmt.Printf("received: %s\n", string(msg.Value))
	}
}

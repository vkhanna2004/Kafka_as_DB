package storage

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

type KafkaConsumer struct {
	consumer *kafka.Consumer
	store    Engine
}

func InitializeKafkaProducer(brokers string) (*KafkaProducer, error) {
	p, error := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
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

func InitializeKafkaConsumer(brokers string, groupID string, m Engine) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
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
	return k.consumer.SubscribeTopics([]string{topic}, func(c *kafka.Consumer, event kafka.Event) error {
		switch ev := event.(type) {
		case kafka.AssignedPartitions:
			for i := range ev.Partitions {
				pOffset, err := k.store.GetPartitionOffset(ev.Partitions[i].Partition)
				if err == nil && pOffset >= 0 {
					ev.Partitions[i].Offset = kafka.Offset(pOffset + 1)
				}
			}

			return c.Assign(ev.Partitions)
		case kafka.RevokedPartitions:
			return c.Unassign()
		}
		return nil
	})
}

func (k *KafkaConsumer) Poll() {

	for {
		msg, err := k.consumer.ReadMessage(-1)

		if err != nil {
			fmt.Println("consumer error:", err)
			continue
		}
		err = k.store.PutWithOffset(string(msg.Key), string(msg.Value), msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))
		if err != nil {
			fmt.Printf("failed to write to store: %v\n", err)
			continue
		}
		fmt.Printf("received: %s (offset: %d)\n", string(msg.Value), msg.TopicPartition.Offset)
	}
}

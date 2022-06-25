package kafka

import (
	"fmt"
	"log"
	"os"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	MsgChan chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *KafkaConsumer {
	return &KafkaConsumer{
		MsgChan: msgChan,
	}
}

func (k *KafkaConsumer) Consumer() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.server": os.Getenv("KafkaBootStrapServers"),
		"group.id":         os.Getenv("KafkaConsumerGroupId"),
	}

	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatal("Error consuming kafka message:" + err.Error())
	}

	topics := []string{os.Getenv("kafkaReadTopics")}
	c.SubscribeTopics(topics, nil)
	fmt.Println("Kafka consumer has been started")

	for {
		message, err := c.ReadMessage(-1)
		if err != nil {
			k.MsgChan <- message
		}
	}
}

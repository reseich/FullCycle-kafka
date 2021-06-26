package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	}
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Println("error consumer", err.Error())
	}
	topics := []string{"test"}
	c.SubscribeTopics(topics, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}

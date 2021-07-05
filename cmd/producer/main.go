package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferiu", "teste", producer, []byte("transferencia"), deliveryChan)
	go DeliveryReport(deliveryChan) //async
	//e := <-deliveryChan
	//msg := e.(*kafka.Message)
	producer.Flush(5000)
}

func NewKafkaProducer() *kafka.Producer{
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "fc2-gokafka_kafka_1:9092",
		"delivery.timeout.ms": "2000", // 0 = infinite
		"acks": "1", //0, 1 ou all
		"enable.idempotence": "false", //when true, acks MUST be "all"
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e:= range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
				// save to database
				// eg: bank account transfer
			}
		}
	}
}

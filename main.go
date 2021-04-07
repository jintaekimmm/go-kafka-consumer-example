package main

import (
	"encoding/json"
	"github.com/99-66/go-kafka-consumer/controllers/kafka"
	"github.com/99-66/go-kafka-consumer/models"
	"log"
	"os"
	"os/signal"
)

func main() {
	kafkaConf, err := kafka.InitKafka()
	if err != nil {
		panic(err)
	}

	consumer, err := kafkaConf.NewConsumer()
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for noti := range consumer.Notifications() {
			log.Printf("Noti: %s\n", noti)
		}
	}()

	for {
		select {
			case msg, ok := <- consumer.Messages():
				if ok {
					var item models.Item
					err = json.Unmarshal(msg.Value, &item)
					if err != nil {
						log.Printf("item %s\n failed unmarshaling. %s\n", msg.Value, err.Error())
					}

					go func() {
						log.Printf("Offset: %d\tItem: %v\n", msg.Offset, item)
					}()
					consumer.MarkOffset(msg, "")
				}

			case <-signals:
				return
		}
	}
}
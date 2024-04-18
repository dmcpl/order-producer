package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/google/uuid"
)

var (
	orderId      = 1000000
	deliveryChan = make(chan kafka.Event)
)

type args struct {
	NumberOfOrders int
}

func main() {
	defer close(deliveryChan)

	a := new(args)
	parseFlags(a)

	s, err := setupSerializer()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "backfill-test",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	o, err := readOrderTemplate()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < a.NumberOfOrders; i++ {
		orderToSend := randomiseFields(*o)

		err = sendToKafka(p, s, "consumer.rewards.orders.backfill", orderToSend)
		if err != nil {
			return
		}
	}
}

func parseFlags(a *args) {
	flag.IntVar(&a.NumberOfOrders, "orders", 1, "number of orders")
	flag.Parse()
}

func randomiseFields(o Order) Order {
	o.UUID = uuid.NewString()
	o.OrdersID = orderId
	orderId++
	return o
}

func readOrderTemplate() (*Order, error) {
	bytes, err := os.ReadFile("order_template.json")
	if err != nil {
		return nil, err
	}

	var order Order
	err = json.Unmarshal(bytes, &order)
	if err != nil {
		return nil, err
	}

	return &order, nil
}

func sendToKafka(p *kafka.Producer, s *avro.GenericSerializer, topic string, o Order) error {
	serialize, err := s.Serialize(topic, &o)
	if err != nil {
		return err
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(o.UUID),
		Value:          serialize,
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Failed to deliver message: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	return nil
}

func setupSerializer() (*avro.GenericSerializer, error) {
	cfg := schemaregistry.NewConfigWithAuthentication("http://localhost:8081", "", "")
	s, err := schemaregistry.NewClient(cfg)
	if err != nil {
		slog.Error("Cannot connect to Schema Registry: %v", err)
		return nil, err
	}

	c := avro.NewSerializerConfig()
	c.UseSchemaID = 4

	d, err := avro.NewGenericSerializer(s, serde.ValueSerde, c)
	if err != nil {
		slog.Error("Cannot create serializer: %v", err)
		return nil, err
	}
	return d, nil
}

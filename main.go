package main

import (
	"context"
	"fmt"
	"github.com/Katherine-988/task_server/logic"
	"github.com/Katherine-988/tools"
	"github.com/segmentio/kafka-go"
	"log"
)

func main() {
	tools.KafkaMgr.Config = &tools.KafkaConfig{
		BrokerAddress: []string{":7087"},
	}

	tools.DBMgr.Config = &tools.DBConfig{
		DSN:                    ":9096",
		MaxIdleConns:           10,
		MaxOpenConns:           10,
		ConnMaxLifetimeSeconds: 0,
		ConnMaxIdleTimeSeconds: 100,
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		GroupID:  "consumer-group-id",
		Topic:    "task_topic",
		MaxBytes: 10e6, // 10MB
	})

	c := context.Background()
	for {
		m, err := r.ReadMessage(c)
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		err = logic.ProcessTask(c, m.Value)
		if err != nil {

		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

	//消费kafka的数据

}

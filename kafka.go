package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func ListTopics(bootstrapServer string) []kafka.Topic {
	conn, err := kafka.Dial("tcp", bootstrapServer)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	// for k := range m {
	// 	fmt.Println(k)
	// }
	keys := make([]string, 0, len(m))
	for k := range m {
		if !strings.HasPrefix(k, "__") {
			keys = append(keys, k)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	client := &kafka.Client{
		Addr:    kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Timeout: 10 * time.Second,
		// Transport: sharedTransport,
	}
	res, _ := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topics: keys,
	})
	for i, v := range res.Topics {
		log.Printf("topic %d: topicname:%s", i, v.Name)
	}
	// rj, _ := json.MarshalIndent(res.Topics, "", "\t")
	// fmt.Printf("%s", rj)
	return res.Topics
}

func ProduceMessage(topic string, partition int) {
	// to produce messages

	conn, err := kafka.DialLeader(context.Background(), "tcp", "0.0.0.0:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("1")},
		kafka.Message{Value: []byte("2")},
		kafka.Message{Value: []byte("3")},
		kafka.Message{Value: []byte("4")},
		kafka.Message{Value: []byte("5")},
		kafka.Message{Value: []byte("6")},
		kafka.Message{Value: []byte("7")},
		kafka.Message{Value: []byte("8")},
		kafka.Message{Value: []byte("9")},
		kafka.Message{Value: []byte("10")},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
func CreateTopic(topicName string, numPartitions, replicationFactor int) {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		},
	}

	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

}
func ChangeCommitOptionOffset(bootstrapServer, topicName, groupID string, offset int64) {
	// auth
	// mechanism, err := scram.Mechanism(scram.SHA512, "username", "password")
	// if err != nil {
	// panic(err)
	// }
	// sharedTransport := &kafka.Transport{
	// SASL: mechanism,
	// }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:                groupID,
		Topics:            []string{topicName},
		Brokers:           []string{bootstrapServer},
		HeartbeatInterval: 2 * time.Second,
		RebalanceTimeout:  2 * time.Second,
		RetentionTime:     time.Hour,
		Logger:            log.New(os.Stdout, "cg-test: ", 0),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer group.Close()

	gen, err := group.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	client := &kafka.Client{
		Addr:    kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Timeout: 10 * time.Second,
		// Transport: sharedTransport,
	}
	ocr, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		Addr:         nil,
		GroupID:      groupID,
		GenerationID: int(gen.ID),
		MemberID:     gen.MemberID,
		Topics: map[string][]kafka.OffsetCommit{
			topicName: {
				{Partition: 0, Offset: offset},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	resps := ocr.Topics[topicName]

	for _, resp := range resps {
		if resp.Error != nil {
			log.Fatal(resp.Error)
		}
		fmt.Printf("resp: %v\n", resp.Partition)
	}

	res, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topicName: {kafka.FirstOffsetOf(0), kafka.LastOffsetOf(0)},
		},
	})

	if err != nil {
		log.Fatal(err)
	}
	partitions, ok := res.Topics[topicName]
	if !ok {
		log.Fatal("missing topic in the list offsets response:", topicName)
	}
	js, _ := json.MarshalIndent(partitions[0], "", "\t")
	log.Printf("%s\n", js)
	re, err := client.ConsumerOffsets(ctx, kafka.TopicAndGroup{
		Topic:   topicName,
		GroupId: groupID,
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	for k, v := range re {
		log.Printf("Parition Num: %d, Value: %d\n", k, v)

	}

}

func Consumer(bootstrapServer, topicName, consumerGroup string) {
	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{bootstrapServer},
		GroupID:  consumerGroup,
		Topic:    topicName,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}
func GetConsumerOffsets(bootstrapServer, topicName, consumerGroup string) (map[int]int64, error) {
	client := &kafka.Client{
		Addr:    kafka.TCP(bootstrapServer),
		Timeout: 10 * time.Second,
		// Transport: sharedTransport,
	}
	re, err := client.ConsumerOffsets(context.Background(), kafka.TopicAndGroup{
		Topic:   topicName,
		GroupId: consumerGroup,
	})
	if err != nil {
		return nil, err
	}
	if len(re) == 0 {
		return nil, errors.New("topic not exist")
	}

	return re, nil

}

func GetConsumerLastOffsets(bootstrapServer, topicName string) {
	client := &kafka.Client{
		Addr:    kafka.TCP(bootstrapServer),
		Timeout: 10 * time.Second,
		// Transport: sharedTransport,
	}

	var consumerOffsets []kafka.OffsetRequest
	for i := 0; i < 10; i++ {
		consumerOffsets = append(consumerOffsets, kafka.LastOffsetOf(i), kafka.FirstOffsetOf(i))
	}
	res, err := client.ListOffsets(context.Background(), &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topicName: consumerOffsets,
		},
	})

	if err != nil {
		log.Fatal("error00001", err)
	}

	for _, v := range res.Topics[topicName] {
		if v.Error != nil {
			log.Fatalf(v.Error.Error())

		}

	}

	partitions, ok := res.Topics[topicName]
	if !ok {
		log.Fatal("missing topic in the list offsets response:", topicName)
	}
	for _, v := range partitions {
		log.Printf("Partition: %d, FirstOffset: %d, LastOffset: %d", v.Partition, v.FirstOffset, v.LastOffset)
	}

}

func GetConsumerOffsetsNew(bootstrapServer, groupId string) {
	client := &kafka.Client{
		Addr:    kafka.TCP(bootstrapServer),
		Timeout: 10 * time.Second,
		// Transport: sharedTransport,
	}
	res, err := client.OffsetFetch(context.Background(), &kafka.OffsetFetchRequest{
		Addr: kafka.TCP(bootstrapServer),
		Topics: map[string][]int{
			"my-test-topic-01": {0, 1},
		},
		GroupID: groupId,
	})
	if err != nil {
		log.Fatal(err)
	}
	for k, v := range res.Topics {
		log.Println("TopicName: ", k)
		for _, sv := range v {
			ts, _ := json.MarshalIndent(sv, "", "\t")
			log.Printf("%s", ts)

		}

	}

}

func TopicDescript(bootstrapServer, topicName string) {
	topics := ListTopics(bootstrapServer)
	for _, v := range topics {
		if v.Name == topicName {
			print("parition number:", len(v.Partitions))

		}
	}
}

func main() {
	// ChangeCommitOptionOffset("localhost:9092", "my-test01", "consumer-group", 45)
	// offsetSet, err := GetConsumerOffsets("localhost:9092", "my-test-topic", "consumer-group")
	// if err != nil {
	// 	log.Fatalf(err.Error())
	// }
	// for k, v := range offsetSet {
	// 	log.Printf("parition: %v, offset: %v\n", k, v)

	// }
	// GetConsumerLastOffsets("localhost:9092", "my-test-topic")
	// Consumer("localhost:9092", "my-test-topic", "consumer-group")
	// CreateTopic("my-test-topic-01", 10, 1)
	// ListTopics()
	// ProduceMessage("my-test-topic-01")
	// GetConsumerOffsetsNew("localhost:9092", "consumer-group")
	// for i := 0; i < 10; i++ {
	// 	ProduceMessage("my-test-topic", i)
	// }
	TopicDescript("localhost:9092", "my-test-topic")
}

package main

import (
	"context"
	"github.com/ritwiksamrat/finalkafkagrpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"fmt"
	"time"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)
type server struct{}

const (
	topic         = "TestTopic"
	brokerAddress = "localhost:9092"
)

func main(){
	listener, err:=net.Listen("tcp",":4040")
	if err!=nil{
		panic(err)
	}
	srv:=grpc.NewServer()
	proto.RegisterProducerServiceServer(srv, &server{})
	reflection.Register(srv)

	if e:=srv.Serve(listener); e!=nil{
		panic(err)
	}
}

func (s *server) Producer(ctx context.Context, request *proto.Request)(*proto.Response, error){
	a:= request.GetUsername()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := "Topic"

	
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(result),
	}, nil)


	p.Flush(15 * 1000)

	return &proto.Response{Result:"success"}, nil
}
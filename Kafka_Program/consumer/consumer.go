package main

import (
	
	"fmt"
	// "log"
	// "os"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	db, err:= sql.Open("mysql", "root:12345@tcp(localhost:3306)/kafkaprog")
	if err != nil {
		fmt.Println("Error creating DB:", err)
    	fmt.Println("To verify, db is:", db)
	}
	defer db.Close()
	fmt.Println("Successfully  Connected to MYSQl")
	

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "smallest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"Topic"}, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			insrt,err:=db.Query("insert into new_table values(?)",string(msg.Value))
			defer insrt.Close()
			if err!=nil{
				panic(err.Error())
			}
			fmt.Println("Successfully inserted")
		} else {
			
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
defer c.Close()
}



package main

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func errHndlr(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func setclasses(appkey string) string {
	return fmt.Sprintf("ns:classes:%s", appkey)
}
func setschema(classes string, appkey string) string {
	return fmt.Sprintf("ns:schema:%s:%s", classes, appkey)
}

func main() {

	conn, err := amqp.Dial("amqp://admin:admin@stage.haru.io:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	defer ch.Close()

	q, err := ch.QueueDeclare( //string, bool, bool, bool, bool, table (Queuem error)
		"schema", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // noWait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	for i := 0; i < 20; i++ {
		go func() {
			c, err := redis.DialTimeout("tcp", "stage.haru.io:6379", time.Duration(10)*time.Second)
			errHndlr(err)
			defer c.Close()
			for {
				for d := range msgs {

					var f interface{}
					err := json.Unmarshal(d.Body, &f)
					errHndlr(err)
					m := f.(map[string]interface{})

					var classname string
					var appkey string
					var keys []interface{}

					for k, v := range m {
						if k == "applicationId" {
							appkey = v.(string)
						}
						if k == "class" {
							classname = v.(string)
						}
						if k == "schema" {
							keys = v.([]interface{})
						}
					}

					// insert classes
					classkey := setclasses(appkey)
					r := c.Cmd("sadd", classkey, classname)
					errHndlr(r.Err)

					// insert schema
					schemakey := setschema(classname, appkey)
					for _, u := range keys {
						r := c.Cmd("sadd", schemakey, u)
						errHndlr(r.Err)
					}

					d.Ack(false)
				}
			}
		}()
	}

	<-forever
}



//
//
//                       _oo0oo_
//                      o8888888o
//                      88" . "88
//                      (| -_- |)
//                      0\  =  /0
//                    ___/`___'\___
//                  .' \\|     |// '.
//                 / \\|||  :  |\\// \
//                / _||||| -:- |||||- \
//               |   | \\\  _  /// |   |
//               | \_|  ''\---/''  |_/ |
//               \   .-\__ '-'  ___/-. /
//             ___'. .'  /--.--\  '. .'___
//            ."" '< '.___\_<|>_/___.' >' "".
//           | | : '- \'.;'\ - /';.'/ - ' : | |
//           \ \  '_.  \_ __\ /__ _/    .-' / /
//       ====='-.____ .__ \_____/ ____.-'___.-'=====
//                        '=---='
//
//      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
//       佛祖保佑 	           永无BUG 			永不修改
//
//
package main

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/streadway/amqp"
	"io"
	"log"
	"os"
	"strings"
)

//logger
var mylogger *log.Logger

func failOnError(err error, msg string) {
	if err != nil {
		mylogger.Fatalf("%s: %s", msg, err)
		//panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func errHndlr(err error) {
	if err != nil {
		mylogger.Println("error:", err)
		//os.Exit(1)
	}
}

func main() {
	logf, err := os.OpenFile("logFileName", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		log.Fatalln(err)
	}
	defer logf.Close()

	log.SetOutput(logf)
	mylogger = log.New(io.MultiWriter(logf, os.Stdout), "schema_worker: ", log.Ldate|log.Ltime|log.Llongfile)
	mylogger.Println("goes to logf")

	conn, err := amqp.Dial("amqp://admin:admin@stage.haru.io:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	defer ch.Close()

	q, err := ch.QueueDeclare( //string, bool, bool, bool, bool, table (Queuem error)
		"schema", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // noWait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	for i := 0; i < 200; i++ {
		go func() {
			c, err := redis.Dial("tcp", "stage.haru.io:6400")
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
						fmt.Println(u)
						strSplit := strings.Split(u.(string), ".")
						r := c.Cmd("hset", schemakey, strSplit[0], strSplit[1])
						errHndlr(r.Err)
					}

					d.Ack(false)
				}
			}
		}()
	}

	<-forever
	//}
}

func setclasses(appkey string) string {
	return fmt.Sprintf("ns:classes:%s", appkey)
}
func setschema(classes string, appkey string) string {
	return fmt.Sprintf("ns:schema:%s:%s", classes, appkey)
}

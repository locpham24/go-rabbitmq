package main

import (
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func createQueue(ch *amqp.Channel, exchangeName string, queueName string, args map[string]interface{}) amqp.Queue {
	logQueue, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Declare binding key
	err = ch.QueueBind(
		logQueue.Name, // queue name
		"",            // routing key
		exchangeName,  // exchange
		false,
		args,
	)
	failOnError(err, "Failed to bind a queue")

	return logQueue
}

func createConsumer(ch *amqp.Channel, queue amqp.Queue) {
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")
	go func() {
		for d := range msgs {
			log.Printf(" [x] From %s: %s", queue.Name, d.Body)
		}
	}()
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "headers_example"
	// Declare the exchange that we want to get message from
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"headers",    // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		map[string]interface{}{
			"format":  "pdf",
			"type":    "report",
			"x-match": "all",
		}, // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	arg1 := map[string]interface{}{
		"format":  "pdf",
		"type":    "report",
		"x-match": "all",
	}

	arg2 := map[string]interface{}{
		"format":  "pdf",
		"type":    "log",
		"x-match": "any",
	}

	arg3 := map[string]interface{}{
		"format":  "zip",
		"type":    "report",
		"x-match": "all",
	}

	queueA := createQueue(ch, exchangeName, "queue_a", arg1)
	queueB := createQueue(ch, exchangeName, "queue_b", arg2)
	queueC := createQueue(ch, exchangeName, "queue_c", arg3)

	forever := make(chan bool)

	createConsumer(ch, queueA)
	createConsumer(ch, queueB)
	createConsumer(ch, queueC)

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

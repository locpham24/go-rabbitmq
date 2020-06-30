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

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare the exchange that we want to get message from
	err = ch.ExchangeDeclare(
		"pdf",   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// Declare the queue to receive message about logging
	logQueue, err := ch.QueueDeclare(
		"pdf_log_queue",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Declare the queue to receive message about creating pdf
	createQueue, err := ch.QueueDeclare(
		"create_pdf_queue",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Declare binding about logging
	err = ch.QueueBind(
		logQueue.Name, // queue name
		"pdf_log",  // routing key
		"pdf", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// Declare binding about creating log
	err = ch.QueueBind(
		createQueue.Name, // queue name
		"pdf_create",  // routing key
		"pdf", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgsFromLog, err := ch.Consume(
		logQueue.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	msgsFromCreate, err := ch.Consume(
		createQueue.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgsFromLog {
			log.Printf(" [x] %s from log", d.Body)
		}
	}()

	go func() {
		for d := range msgsFromCreate {
			log.Printf(" [x] %s from create", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
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

func createQueue(ch *amqp.Channel, exchangeName string, queueName string, routingPattern string) amqp.Queue {
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
		logQueue.Name,  // queue name
		routingPattern, // routing key
		exchangeName,   // exchange
		false,
		nil,
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

	exchangeName := "sport_news"
	// Declare the exchange that we want to get message from
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	berlinQueue := createQueue(ch, exchangeName, "berlin_agreements", "agreements.eu.berlin.#")
	allQueue := createQueue(ch, exchangeName, "all_agreements", "agreements.#")
	headstoreQueue := createQueue(ch, exchangeName, "headstore_agreements", "agreements.eu.*.headstore")

	forever := make(chan bool)

	createConsumer(ch, berlinQueue)
	createConsumer(ch, allQueue)
	createConsumer(ch, headstoreQueue)

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

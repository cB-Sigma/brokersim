// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package brokersim_test

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cB-Sigma/brokersim/amqp091sim"
)

// Start a RabbitMQ-like broker, publish through the real client and look
// at what the broker received.
func Example() {
	b, err := amqp091sim.Start(amqp091sim.Options{})
	if err != nil {
		panic(err)
	}
	defer b.Close()
	b.DeclareQueue("readings", "amq.topic", "meters.#")

	conn, err := amqp.Dial("amqp://guest:guest@" + b.Addr() + "/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "amq.topic", "meters.m1", false, false, amqp.Publishing{Body: []byte(`{"kWh":12.5}`)}); err != nil {
		panic(err)
	}
	for i := 0; i < 100 && len(b.Published()) == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	ready, _ := b.Depth("readings")
	fmt.Println(b.Published()[0].RoutingKey, string(b.Published()[0].Body), ready)
	// Output: meters.m1 {"kWh":12.5} 1
}

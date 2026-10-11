// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package brokersim holds in-process message broker fakes for tests, each
// in its own package:
//
//   - amqpsim: an AMQP 1.0 broker with Service Bus style queues, Event Hubs
//     style partitioned hubs, the $management and $cbs nodes and SASL;
//   - amqp091sim: an AMQP 0-9-1 broker with RabbitMQ's exchanges, queues,
//     acknowledgements, publisher confirms and a management API handler;
//   - entrasim: a stand-in for the Microsoft identity platform's token
//     endpoint (OAuth 2.0 client credentials).
//
// Each starts on 127.0.0.1 with an ephemeral port, so tests can run in
// parallel; cmd/brokersim runs them on fixed addresses.
package brokersim

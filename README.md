# brokersim

In-process message broker fakes for Go tests: AMQP 1.0 that behaves like Azure Service Bus and Event Hubs, AMQP 0-9-1 that behaves like RabbitMQ, and the Microsoft Entra ID token endpoint.

[![Go Reference](https://pkg.go.dev/badge/github.com/cB-Sigma/brokersim.svg)](https://pkg.go.dev/github.com/cB-Sigma/brokersim) [![test](https://github.com/cB-Sigma/brokersim/actions/workflows/test.yml/badge.svg)](https://github.com/cB-Sigma/brokersim/actions/workflows/test.yml) [![Licence: Apache-2.0](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE) [![Go](https://img.shields.io/badge/go-1.26-00ADD8.svg)](go.mod)

Real clients (github.com/Azure/go-amqp, github.com/rabbitmq/amqp091-go,
any OAuth 2.0 client) connect to the fakes over TCP on 127.0.0.1, so a test
runs the same client code as production, without Docker, in milliseconds
and in parallel. Pure Go, no cgo. Written for SigmaM2M, a oneM2M platform,
whose stream, CIM, forwarding and Azure IoT Hub adapters are tested
against it.

## Features

| Feature | Status |
|---|---|
| AMQP 1.0: open, begin, attach, flow, transfer, disposition, detach, end, close; link credit; settlement modes; SASL PLAIN and ANONYMOUS | yes |
| Service Bus style queues: taken on accept, back with a raised delivery count on release, modify or a lost link; `amqp:not-found` for unknown addresses; `amqp:rejected` from a hook | yes |
| Event Hubs style hubs: partitioned log, offset, sequence number and enqueued-time annotations, selector filter, `$management` READ of partition IDs | yes |
| Claims-based security (`$cbs` put-token, checked by a hook); the entity feed listing (`ResourcesHandler`, SAS token checked) | yes |
| AMQP 1.0 transactions, message sessions, scheduled messages, dead-lettering, TLS | no |
| AMQP 0-9-1: default, direct, topic and fanout exchanges; durable and exclusive queues; `*` and `#` bindings; prefetch; ack, nack and reject (multiple); redelivered flag | yes |
| Publisher confirms, `basic.return`, PLAIN and AMQPLAIN logins, channel errors; the RabbitMQ management API's queue and binding listings (`ManagementHandler`) | yes |
| `basic.get`, headers exchanges, `tx.*`, TLS, persistence across restarts | no |
| Entra ID: client credentials grant with a client secret, the platform's error codes | yes |
| Entra ID: certificates, managed identity, other grants | no |
| Kafka | no: use franz-go's `kfake` |

## Choosing a broker fake

Compared from each project's source at the versions named, in October
2026. Check the projects themselves before relying on a row.

| | this module | real broker in Docker (testcontainers-go v0.44.0) | Service Bus and Event Hubs emulators (Microsoft) | NeowayLabs/wabbit `amqptest` (master 73ad61d, 2021) | Azure/go-amqp `internal/fake` |
|---|---|---|---|---|---|
| Needs Docker | no | yes | yes | no | no |
| Speaks the wire protocol (your production client connects) | yes, over TCP | yes | yes | no: your code must use wabbit's interfaces | no: it fakes a `net.Conn` for go-amqp's own tests |
| AMQP 1.0 with Service Bus / Event Hubs addressing and annotations | yes | n/a | yes, the real behaviour | no | frames only |
| AMQP 0-9-1 with RabbitMQ exchanges and confirms | yes | yes, the real broker | no | in memory, through its interfaces | no |
| Entra ID token endpoint | yes | no | no | no | no |
| Test hooks: inspect published messages and queue depth, reject deliveries, check tokens | yes | through the management API | no | not compared | n/a |
| Importable from another module | yes | yes | n/a (closed-source images) | yes | no (internal package) |

Pick this module for fast unit and integration tests of code that uses
the real Go clients. Pick a real RabbitMQ under testcontainers-go, or
Microsoft's emulators, before a release, to catch behaviour this fake does
not have (the "no" rows above), and the services themselves for anything
that depends on quotas, throttling or TLS.

## Install

```
go get github.com/cB-Sigma/brokersim@latest
```

Requires Go 1.26 or later.

## Quick start

```go
import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/cB-Sigma/brokersim/amqp091sim"
)

b, err := amqp091sim.Start(amqp091sim.Options{}) // guest/guest, 127.0.0.1, ephemeral port
defer b.Close()
b.DeclareQueue("readings", "amq.topic", "meters.#")

conn, err := amqp.Dial("amqp://guest:guest@" + b.Addr() + "/")
ch, err := conn.Channel()
err = ch.PublishWithContext(ctx, "amq.topic", "meters.m1", false, false, amqp.Publishing{Body: []byte(`{"kWh":12.5}`)})

// b.Published()[0].RoutingKey == "meters.m1"; b.Depth("readings") == 1
```

This is `Example` in example_test.go, which runs and checks its output.
To run the fakes as processes, on fixed addresses:

```
go run ./cmd/brokersim -amqp 127.0.0.1:5672 -hub telemetry:4 -amqp091 127.0.0.1:5673 -mgmt 127.0.0.1:15672 -entra
```

## Documentation

API reference: https://pkg.go.dev/github.com/cB-Sigma/brokersim

| Package | What it does |
|---|---|
| `amqpsim` | AMQP 1.0 broker: Service Bus style queues, Event Hubs style partitioned hubs, `$management`, `$cbs`, SASL, `ResourcesHandler` |
| `amqp091sim` | AMQP 0-9-1 broker with RabbitMQ's exchanges, queues, acknowledgements and confirms; `ManagementHandler` |
| `entrasim` | Microsoft identity platform token endpoint, client credentials grant |
| `cmd/brokersim` | runs any of them on fixed addresses |

## Testing

```
go test -race ./...
```

### Interop tested against

- github.com/Azure/go-amqp v1.7.0 and github.com/rabbitmq/amqp091-go
  v1.15.0, the real clients, in this module's tests and in SigmaM2M's
  adapter tests.
- The fakes were written from the AMQP specifications and the services'
  public documentation. They have not been compared message by message
  with a live Service Bus, Event Hubs, RabbitMQ or Entra ID tenant, nor
  with Microsoft's emulators: a behaviour you depend on that is not in the
  tables above should be checked against the real service.

## Related repositories

None. brokersim is itself a set of simulators.

## Status

v0.x: the API may change before v1.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md) to report a vulnerability privately. These
are test fakes: do not expose them on a network.

## Licence

Apache-2.0 (see [LICENSE](LICENSE) and [NOTICE](NOTICE)). Copyright 2026
Abhinav Gautam. Azure, Service Bus, Event Hubs, Microsoft Entra and
RabbitMQ are trademarks of their owners; this module is not made or
endorsed by them.

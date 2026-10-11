# Changelog

## v0.2.0 (2026-10-12)

amqpsim (Service Bus and Event Hubs):

- Peek-lock with lock tokens, `x-opt-locked-until` and sequence numbers;
  complete, abandon, defer and dead-letter outcomes; the
  `$DeadLetterQueue` subqueue with reason and description; maximum
  delivery count (`QueueOptions`, `CreateQueueWith`, `DeadLetters`).
- Scheduled messages (annotation, `$management` schedule and cancel),
  lock and session lock renewal, message sessions.
- Event Hubs batched sends, partition properties, `max-message-size`.
- `Options.TLS`, `Options.Throttle` (`com.microsoft:server-busy`),
  `Options.MaxSize` (`amqp:link:message-size-exceeded`).

amqp091sim (RabbitMQ):

- Headers exchanges, `basic.get`, `tx.*`, queue purge, delete and unbind,
  exchange delete.
- Dead-letter exchanges with `x-death`, `x-message-ttl`, `x-max-length`.
- `Options.TLS`; `Snapshot` and `Options.Restore` keep durable queues,
  exchanges and bindings across a restart.
- Arrays, timestamps and floats in field tables.

entrasim (Entra ID):

- Certificate credentials (client assertions signed RS256, PS256 or
  ES256), `Server.Certificates`; OpenID metadata; managed identity (IMDS
  and App Service, `Server.IdentityHeader`); `StartTLS`.

New packages: `kafkasim` (the Event Hubs Kafka endpoint, on franz-go's
`kfake`) and `testcert`.

New `interop` module: RabbitMQ 4.1.4 side by side, and the Azure SDK for
Go (azservicebus, azeventhubs, azidentity) against the fakes.

## v0.1.0 (2026-10-06)

First release: amqpsim, amqp091sim, entrasim and cmd/brokersim.

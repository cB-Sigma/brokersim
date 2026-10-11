# Provenance

## How the code was written

Written for SigmaM2M in Go as test fakes for its broker clients (stream,
CIM, forwarding and Azure IoT Hub adapters). No code was ported or copied
from another implementation, and no source file of another project was
translated. The three packages moved here unchanged from SigmaM2M's
server on 2026-10-06; `StartOn` (a chosen listen address, for
cmd/brokersim) was added to amqpsim and amqp091sim. doc.go, the entrasim
test, the example and cmd/brokersim were written for this module.

v0.2.0 (2026-10-12) was written for this module from the sources below:
the Service Bus and Event Hubs additions to amqpsim, the RabbitMQ
additions and snapshot.go in amqp091sim, more.go in entrasim, kafkasim,
testcert and the interop module. Where the documentation left a detail
open, the behaviour was checked by running the real clients against the
fake (the Azure SDK for Go) or the fake and the real broker side by side
(RabbitMQ 4.1.4); no code of those projects was copied. kafkasim is
configuration of franz-go's kfake, used as a dependency.

## Sources

- amqpsim: OASIS AMQP 1.0 (types, transport, messaging and security
  parts: frames, performatives, link credit, settlement, SASL), the AMQP
  Claims-Based Security working draft (put-token), and Microsoft's public
  documentation of AMQP 1.0 in Service Bus and Event Hubs (entity
  addresses, partition addressing, the x-opt-* annotations, the selector
  filter, error conditions) and of the Service Bus entity feeds; for
  v0.2.0 also its documentation of message locks, settlement,
  dead-letter queues, scheduled messages, sessions, the AMQP management
  operations and the Event Hubs Kafka endpoint.
- amqp091sim: the AMQP 0-9-1 specification and its XML method table
  (class and method IDs, field orders are interface facts), and RabbitMQ's
  public documentation (exchange types, publisher confirms, reply codes,
  the management HTTP API's queue and binding listings; for v0.2.0 also
  headers exchanges, transactions, dead-letter exchanges, TTL and queue
  length limits).
- entrasim: Microsoft identity platform documentation of the OAuth 2.0
  client credentials grant (RFC 6749 section 4.4) and its error codes;
  for v0.2.0 also certificate credentials (RFC 7523, RFC 7515 for the
  JWT and its x5t headers), OpenID Connect discovery metadata and the
  managed identity endpoints.

Wording in the code is our own; clause and section numbers only.

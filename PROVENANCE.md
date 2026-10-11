# Provenance

## How the code was written

Written for SigmaM2M in Go as test fakes for its broker clients (stream,
CIM, forwarding and Azure IoT Hub adapters). No code was ported or copied
from another implementation, and no source file of another project was
translated. The three packages moved here unchanged from SigmaM2M's
server on 2026-10-06; `StartOn` (a chosen listen address, for
cmd/brokersim) was added to amqpsim and amqp091sim. doc.go, the entrasim
test, the example and cmd/brokersim were written for this module.

## Sources

- amqpsim: OASIS AMQP 1.0 (types, transport, messaging and security
  parts: frames, performatives, link credit, settlement, SASL), the AMQP
  Claims-Based Security working draft (put-token), and Microsoft's public
  documentation of AMQP 1.0 in Service Bus and Event Hubs (entity
  addresses, partition addressing, the x-opt-* annotations, the selector
  filter, error conditions) and of the Service Bus entity feeds.
- amqp091sim: the AMQP 0-9-1 specification and its XML method table
  (class and method IDs, field orders are interface facts), and RabbitMQ's
  public documentation (exchange types, publisher confirms, reply codes,
  the management HTTP API's queue and binding listings).
- entrasim: Microsoft identity platform documentation of the OAuth 2.0
  client credentials grant (RFC 6749 section 4.4) and its error codes.

Wording in the code is our own; clause and section numbers only.

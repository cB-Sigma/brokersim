// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package amqp091sim

import (
	"encoding/json"
	"time"
)

type snapExchange struct {
	VHost, Name, Kind string
	Bindings          []snapBinding
}

type snapBinding struct {
	Queue, Key string
	Args       map[string]any `json:",omitempty"`
}

type snapQueue struct {
	VHost, Name string
	Args        map[string]any `json:",omitempty"`
	Messages    []Message
}

type snapshot struct {
	Exchanges []snapExchange
	Queues    []snapQueue
}

// Snapshot records what a RabbitMQ restart keeps: durable exchanges and
// queues, the bindings between them and the queues' messages
// (unacknowledged ones as ready and redelivered). Pass it as
// Options.Restore to start a broker in that state.
func (b *Broker) Snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	var s snapshot
	durable := map[string]bool{}
	for _, q := range b.queues {
		if !q.durable {
			continue
		}
		durable[q.vhost+"|"+q.name] = true
		sq := snapQueue{VHost: q.vhost, Name: q.name, Args: q.args}
		for c := range b.conns {
			for _, ch := range c.channels {
				for _, u := range ch.unacked {
					if u.q == q {
						m := u.m
						m.Redelivered = true
						sq.Messages = append(sq.Messages, m)
					}
				}
			}
		}
		sq.Messages = append(sq.Messages, q.ready...)
		s.Queues = append(s.Queues, sq)
	}
	for k, e := range b.exchanges {
		vh := k[:len(k)-len(e.name)-1]
		if !e.durable && !builtin(e.name) {
			continue
		}
		se := snapExchange{VHost: vh, Name: e.name, Kind: e.kind}
		for _, bd := range e.bindings {
			if durable[vh+"|"+bd.queue] {
				se.Bindings = append(se.Bindings, snapBinding{Queue: bd.queue, Key: bd.key, Args: bd.args})
			}
		}
		s.Exchanges = append(s.Exchanges, se)
	}
	out, _ := json.Marshal(s)
	return out
}

func builtin(name string) bool {
	switch name {
	case "", "amq.direct", "amq.topic", "amq.fanout", "amq.headers", "amq.match":
		return true
	}
	return false
}

func (b *Broker) restore(data []byte) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	for _, sq := range s.Queues {
		q := b.queueLocked(sq.VHost, sq.Name, true)
		q.durable, q.args, q.ready = true, sq.Args, sq.Messages
		for i := range q.ready {
			q.ready[i].enqueued = time.Now()
		}
	}
	for _, se := range s.Exchanges {
		e := b.exchanges[se.VHost+"|"+se.Name]
		if e == nil {
			e = &exchange{name: se.Name, kind: se.Kind, durable: true}
			b.exchanges[se.VHost+"|"+se.Name] = e
		}
		for _, bd := range se.Bindings {
			e.bindings = append(e.bindings, binding{queue: bd.Queue, key: bd.Key, args: bd.Args})
		}
	}
	return nil
}

// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package amqpsim

// Service Bus queue behaviour added in v0.2.0, from Service Bus's public
// documentation and what its AMQP clients send: sequence numbers and
// enqueued times, lock tokens and locked-until times, abandon (the
// delivery count raised), defer, dead-lettering to the queue's
// $DeadLetterQueue subqueue (on request with a reason and description, or
// past the maximum delivery count), scheduled messages (the
// x-opt-scheduled-enqueue-time annotation or the
// com.microsoft:schedule-message operation), message sessions (the
// com.microsoft:session-filter on a receiver, one lock per session), lock
// renewal, and the Event Hubs partition properties and batched sends.

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// QueueOptions configure a Service Bus style queue.
type QueueOptions struct {
	// RequiresSession makes the queue deliver by session (group ID).
	RequiresSession bool
	// MaxDeliveryCount moves a message to the dead-letter subqueue after
	// this many deliveries (default 10).
	MaxDeliveryCount uint32
	// LockDuration is how long a peek-locked message stays locked (default
	// one minute); it is reported, not enforced.
	LockDuration time.Duration
}

func (o QueueOptions) withDefaults() QueueOptions {
	if o.MaxDeliveryCount == 0 {
		o.MaxDeliveryCount = 10
	}
	if o.LockDuration <= 0 {
		o.LockDuration = time.Minute
	}
	return o
}

// CreateQueueWith creates a queue with Service Bus options.
func (b *Broker) CreateQueueWith(name string, o QueueOptions) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[name]
	if q == nil {
		q = &queue{name: name}
		b.queues[name] = q
	}
	q.opts = o.withDefaults()
}

// DeadLetters returns the messages in a queue's dead-letter subqueue.
func (b *Broker) DeadLetters(queueName string) []*amqp.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[queueName]
	if q == nil || q.dlq == nil {
		return nil
	}
	var out []*amqp.Message
	for _, st := range q.dlq.ready {
		out = append(out, st.msg)
	}
	return out
}

const dlqSuffix = "/$deadletterqueue"

// queueFor finds the queue an address names: a queue, or a queue's
// dead-letter subqueue.
func (b *Broker) queueFor(address string) *queue {
	if q := b.queues[address]; q != nil {
		return q
	}
	if strings.HasSuffix(strings.ToLower(address), dlqSuffix) {
		base := b.queues[address[:len(address)-len(dlqSuffix)]]
		if base == nil {
			return nil
		}
		if base.dlq == nil {
			base.dlq = &queue{name: base.name + "/$DeadLetterQueue", opts: QueueOptions{}.withDefaults()}
		}
		return base.dlq
	}
	return nil
}

func (q *queue) options() QueueOptions { return q.opts.withDefaults() }

// enqueueLocked stamps a message for a queue (sequence number, enqueued
// time; scheduled time and session from the message) and adds it.
func (b *Broker) enqueueLocked(q *queue, m *amqp.Message) *stored {
	cp := *m
	ann := amqp.Annotations{}
	for k, v := range cp.Annotations {
		ann[k] = v
	}
	st := &stored{seq: q.nextSeq, enqueued: time.Now().UTC().Truncate(time.Millisecond)}
	q.nextSeq++
	if t, ok := annTime(ann, "x-opt-scheduled-enqueue-time"); ok && t.After(time.Now()) {
		st.visibleAt = t
		b.wakeAt(t)
	}
	ann[amqp.Symbol("x-opt-sequence-number")] = st.seq
	ann[amqp.Symbol("x-opt-enqueued-time")] = st.enqueued
	cp.Annotations = ann
	if cp.Properties != nil && cp.Properties.GroupID != nil {
		st.session = *cp.Properties.GroupID
	}
	st.msg = &cp
	st.payload, _ = cp.MarshalBinary()
	q.ready = append(q.ready, st)
	return st
}

// wakeAt pumps again when a scheduled message becomes visible.
func (b *Broker) wakeAt(t time.Time) {
	time.AfterFunc(time.Until(t)+5*time.Millisecond, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if !b.closed {
			b.pumpLocked()
		}
	})
}

// takeLocked removes and returns the next message the link may get: the
// first visible one, of the link's session on a session queue.
func (q *queue) takeLocked(l *link) *stored {
	now := time.Now()
	for i, st := range q.ready {
		if !st.visibleAt.IsZero() && now.Before(st.visibleAt) {
			continue
		}
		if q.options().RequiresSession && st.session != l.session {
			continue
		}
		q.ready = append(q.ready[:i:i], q.ready[i+1:]...)
		return st
	}
	return nil
}

// deadLetterLocked moves a message to its queue's dead-letter subqueue
// with the reason and description as application properties.
func (b *Broker) deadLetterLocked(q *queue, st *stored, reason, desc string) {
	if q.dlq == nil {
		if strings.HasSuffix(strings.ToLower(q.name), dlqSuffix) {
			return // already a dead letter: it stays settled
		}
		q.dlq = &queue{name: q.name + "/$DeadLetterQueue", opts: QueueOptions{}.withDefaults()}
	}
	cp := *st.msg
	props := map[string]any{}
	for k, v := range cp.ApplicationProperties {
		props[k] = v
	}
	props["DeadLetterReason"] = reason
	props["DeadLetterErrorDescription"] = desc
	cp.ApplicationProperties = props
	ann := amqp.Annotations{}
	for k, v := range cp.Annotations {
		ann[k] = v
	}
	ann[amqp.Symbol("x-opt-deadletter-source")] = q.name
	cp.Annotations = ann
	payload, _ := cp.MarshalBinary()
	q.dlq.ready = append(q.dlq.ready, &stored{payload: payload, msg: &cp, count: st.count, seq: st.seq, enqueued: st.enqueued, session: st.session})
}

// redeliverLocked puts an abandoned or released message back with its
// delivery count raised, or dead-letters it past the maximum.
func (b *Broker) redeliverLocked(q *queue, st *stored) {
	st.count++
	if st.count >= q.options().MaxDeliveryCount {
		b.deadLetterLocked(q, st, "MaxDeliveryCountExceeded", "Message could not be consumed after "+strconv.Itoa(int(st.count))+" delivery attempts.")
		return
	}
	q.ready = append([]*stored{st}, q.ready...)
}

// lockToken is a delivery tag: a random GUID, which Service Bus clients
// read as the message's lock token.
func lockToken() []byte {
	t := make([]byte, 16)
	_, _ = rand.Read(t)
	return t
}

// dotnetTicks is a time in .NET ticks (100 ns since 0001-01-01), as
// Service Bus sends com.microsoft:locked-until-utc.
func dotnetTicks(t time.Time) int64 {
	return t.UTC().UnixNano()/100 + 621355968000000000
}

// deliveryPayload is a queue message as it goes out: the header's
// delivery count (deliveries before this one) and the lock's expiry.
func deliveryPayload(q *queue, st *stored) []byte {
	cp := *st.msg
	hdr := amqp.MessageHeader{DeliveryCount: st.count}
	if cp.Header != nil {
		hdr = *cp.Header
		hdr.DeliveryCount = st.count
	}
	cp.Header = &hdr
	ann := amqp.Annotations{}
	for k, v := range cp.Annotations {
		ann[k] = v
	}
	ann[amqp.Symbol("x-opt-locked-until")] = time.Now().Add(q.options().LockDuration).UTC().Truncate(time.Millisecond)
	cp.Annotations = ann
	payload, _ := cp.MarshalBinary()
	return payload
}

// serviceBusManagementLocked answers the Service Bus operations on an
// entity's $management node; false for an operation it does not know.
func (b *Broker) serviceBusManagementLocked(c *conn, m *amqp.Message, op string, q *queue) bool {
	switch op {
	case "com.microsoft:schedule-message":
		v, _ := m.Value.(map[string]any)
		list, _ := v["messages"].([]any)
		var seqs []int64
		for _, item := range list {
			im, _ := item.(map[string]any)
			raw, _ := im["message"].([]byte)
			var sm amqp.Message
			if err := sm.UnmarshalBinary(raw); err != nil {
				b.replyLocked(c, m, 400, "bad message: "+err.Error(), nil)
				return true
			}
			st := b.enqueueLocked(q, &sm)
			seqs = append(seqs, st.seq)
		}
		b.replyLocked(c, m, 200, "OK", map[string]any{"sequence-numbers": seqs})
		return true
	case "com.microsoft:cancel-scheduled-message":
		v, _ := m.Value.(map[string]any)
		seqs, _ := v["sequence-numbers"].([]int64)
		kept := q.ready[:0]
		for _, st := range q.ready {
			cancel := false
			for _, s := range seqs {
				cancel = cancel || (s == st.seq && !st.visibleAt.IsZero())
			}
			if !cancel {
				kept = append(kept, st)
			}
		}
		q.ready = kept
		b.replyLocked(c, m, 200, "OK", nil)
		return true
	case "com.microsoft:renew-lock":
		v, _ := m.Value.(map[string]any)
		toks, _ := v["lock-tokens"].([]amqp.UUID)
		exp := make([]time.Time, len(toks))
		for i := range exp {
			exp[i] = time.Now().Add(q.options().LockDuration).UTC().Truncate(time.Millisecond)
		}
		b.replyLocked(c, m, 200, "OK", map[string]any{"expirations": exp})
		return true
	case "com.microsoft:renew-session-lock":
		b.replyLocked(c, m, 200, "OK", map[string]any{"expiration": time.Now().Add(q.options().LockDuration).UTC().Truncate(time.Millisecond)})
		return true
	}
	return false
}

// partitionPropertiesLocked answers an Event Hubs READ of
// com.microsoft:partition.
func (b *Broker) partitionPropertiesLocked(c *conn, m *amqp.Message, h *hub) {
	pid, _ := m.ApplicationProperties["partition"].(string)
	p, err := strconv.Atoi(pid)
	if err != nil || p < 0 || p >= len(h.partitions) {
		b.replyLocked(c, m, 404, "The partition '"+pid+"' could not be found.", nil)
		return
	}
	evs := h.partitions[p]
	v := map[string]any{"name": h.name, "type": "com.microsoft:partition", "partition": pid,
		"begin_sequence_number": int64(0), "last_enqueued_sequence_number": int64(-1),
		"last_enqueued_offset": "-1", "last_enqueued_time_utc": time.Unix(0, 0).UTC(), "is_partition_empty": len(evs) == 0}
	if len(evs) > 0 {
		last := evs[len(evs)-1]
		v["last_enqueued_sequence_number"] = last.seq
		v["last_enqueued_offset"] = strconv.FormatInt(last.offset, 10)
		v["last_enqueued_time_utc"] = last.enqueued
	}
	b.replyLocked(c, m, 200, "OK", v)
}

// batchFormat is the message format of an Event Hubs batch: each data
// section holds one encoded message.
const batchFormat = 0x80013700

// unbatch returns the messages of a batch, or m itself.
func unbatch(format uint32, m *amqp.Message) []*amqp.Message {
	if format != batchFormat {
		return []*amqp.Message{m}
	}
	var out []*amqp.Message
	for _, d := range m.Data {
		var inner amqp.Message
		if inner.UnmarshalBinary(d) == nil {
			out = append(out, &inner)
		}
	}
	return out
}

// condError refuses a message with an AMQP error condition of its own.
type condError struct{ cond, desc string }

func (e *condError) Error() string { return e.desc }

// isNode reports a $management or $cbs node address (a Service Bus entity's
// own management node included).
func isNode(a string) bool {
	return a == "$management" || a == "$cbs" || strings.HasSuffix(a, "/$management")
}

// lockSessionLocked gives a receiver the lock of a session: the one
// named, or with next the first session with a message waiting that no
// one holds. It returns the session ID, or why the lock was refused.
func (q *queue) lockSessionLocked(l *link, sid string, next bool) (string, string) {
	if q.locks == nil {
		q.locks = map[string]*link{}
	}
	if next {
		now := time.Now()
		for _, st := range q.ready {
			if st.session != "" && q.locks[st.session] == nil && (st.visibleAt.IsZero() || !now.Before(st.visibleAt)) {
				sid = st.session
				break
			}
		}
		if sid == "" {
			return "", "No unlocked session with messages is available."
		}
	} else if q.locks[sid] != nil {
		return "", "The requested session '" + sid + "' cannot be accepted. It may be locked by another receiver."
	}
	q.locks[sid] = l
	l.session = sid
	return sid, ""
}

// settleQueueLocked applies a receiver's outcome to a queue message:
// accepted completes it; released or modified (abandon) puts it back with
// its delivery count raised; modified as undeliverable here defers it;
// rejected dead-letters it (Service Bus clients send
// com.microsoft:dead-letter with the reason and description in the
// error's info).
func (b *Broker) settleQueueLocked(q *queue, st *stored, state Described) {
	fields, _ := state.Value.([]any)
	switch state.Code {
	case codeAccepted:
	case codeModified:
		if asBool(field(fields, 1)) {
			q.deferred = append(q.deferred, st)
			return
		}
		b.redeliverLocked(q, st)
	case codeReleased:
		b.redeliverLocked(q, st)
	case codeRejected:
		reason, desc := "rejected", ""
		if e, ok := field(fields, 0).(Described); ok {
			el, _ := e.Value.([]any)
			reason, desc = asString(field(el, 0)), asString(field(el, 1))
			if info, ok := field(el, 2).(map[any]any); ok {
				for k, v := range info {
					switch asString(k) {
					case "DeadLetterReason":
						reason = asString(v)
					case "DeadLetterErrorDescription":
						desc = asString(v)
					}
				}
			}
		}
		b.deadLetterLocked(q, st, reason, desc)
	}
}

// annTime reads a time annotation whatever the key's Go type (symbol or
// string) and the value's (time or pointer to time).
func annTime(ann amqp.Annotations, key string) (time.Time, bool) {
	for k, v := range ann {
		if fmt.Sprint(k) != key {
			continue
		}
		switch t := v.(type) {
		case time.Time:
			return t, true
		case *time.Time:
			return *t, t != nil
		}
	}
	return time.Time{}, false
}

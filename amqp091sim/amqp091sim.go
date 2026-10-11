// Package amqp091sim is an in-process AMQP 0-9-1 broker for tests, built
// from the AMQP 0-9-1 specification (frames, methods, content headers,
// field tables) and RabbitMQ's documented behaviour: topic, direct and
// fanout exchanges (amq.topic, amq.direct, amq.fanout and the default
// exchange), durable and exclusive queues, bindings with * and #, consumers
// with a prefetch window, acknowledgements (ack, nack, reject; multiple),
// redelivery with the redelivered flag when a channel closes with
// unacknowledged messages, publisher confirms with basic.return for an
// unroutable mandatory message, PLAIN and AMQPLAIN logins refused with
// 403 ACCESS_REFUSED, and channel errors (404 NOT_FOUND for a missing
// exchange or queue).
//
// References: AMQP 0-9-1 specification and XML (https://www.rabbitmq.com/
// resources/specs/amqp0-9-1.pdf), RabbitMQ publisher confirms
// (https://www.rabbitmq.com/docs/confirms), RabbitMQ topic exchanges
// (https://www.rabbitmq.com/tutorials/amqp-concepts).
package amqp091sim

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

const frameEnd = 0xCE

// Message is a message in the broker.
type Message struct {
	Exchange    string
	RoutingKey  string
	Body        []byte
	Headers     map[string]any
	ContentType string
	Correlation string
	ReplyTo     string
	MessageID   string
	Timestamp   time.Time
	Redelivered bool
}

// Options configure the broker.
type Options struct {
	// Users maps user names to passwords; empty accepts guest/guest only.
	Users map[string]string
	// VHosts are the virtual hosts (default "/").
	VHosts []string
}

// Broker is the in-process broker.
type Broker struct {
	opts Options
	ln   net.Listener

	mu        sync.Mutex
	exchanges map[string]*exchange // vhost|name
	queues    map[string]*queue    // vhost|name
	published []Message
	conns     map[*conn]bool
	closed    bool
	wg        sync.WaitGroup
	nextQ     int
}

type exchange struct {
	name, kind string
	bindings   []binding
}

type binding struct {
	queue, key string
}

type queue struct {
	name      string
	vhost     string
	ready     []Message
	consumers []*consumer
	next      int // round robin
	exclusive *conn
	unacked   int
}

type consumer struct {
	tag   string
	ch    *channel
	q     *queue
	noAck bool
}

// Start starts a broker on 127.0.0.1 and an ephemeral port.
func Start(o Options) (*Broker, error) { return StartOn("127.0.0.1:0", o) }

// StartOn starts a broker listening on addr (host:port).
func StartOn(addr string, o Options) (*Broker, error) {
	if len(o.Users) == 0 {
		o.Users = map[string]string{"guest": "guest"}
	}
	if len(o.VHosts) == 0 {
		o.VHosts = []string{"/"}
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	b := &Broker{opts: o, ln: ln, exchanges: map[string]*exchange{}, queues: map[string]*queue{}, conns: map[*conn]bool{}}
	for _, vh := range o.VHosts {
		for _, e := range []struct{ n, k string }{{"", "direct"}, {"amq.direct", "direct"}, {"amq.topic", "topic"}, {"amq.fanout", "fanout"}} {
			b.exchanges[vh+"|"+e.n] = &exchange{name: e.n, kind: e.k}
		}
	}
	b.wg.Add(1)
	go b.accept()
	return b, nil
}

// Addr is the broker's host:port.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// Close stops the broker.
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for c := range b.conns {
		c.nc.Close()
	}
	b.mu.Unlock()
	b.ln.Close()
	b.wg.Wait()
}

// DropConnections closes every client connection, as a broker restart or
// a network cut does.
func (b *Broker) DropConnections() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.conns {
		c.nc.Close()
	}
}

// DeclareQueue creates a durable queue (vhost "/") bound to exchange with
// key, as an operator would.
func (b *Broker) DeclareQueue(name, exchange, key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queueLocked("/", name, true)
	if exchange != "" || key != "" {
		if e := b.exchanges["/|"+exchange]; e != nil {
			e.bindings = append(e.bindings, binding{queue: q.name, key: key})
		}
	}
}

// Publish routes a message as a client's basic.publish would (vhost
// "/"); it returns how many queues took it.
func (b *Broker) Publish(exchange, key string, m Message) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	m.Exchange, m.RoutingKey = exchange, key
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	return b.routeLocked("/", m)
}

// Published returns every message clients published, in order.
func (b *Broker) Published() []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Message(nil), b.published...)
}

// Depth returns a queue's ready and unacknowledged message counts.
func (b *Broker) Depth(queue string) (ready, unacked int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues["/|"+queue]
	if q == nil {
		return 0, 0
	}
	return len(q.ready), q.unacked
}

// Queues lists the queue names of vhost "/".
func (b *Broker) Queues() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, q := range b.queues {
		if q.vhost == "/" {
			out = append(out, q.name)
		}
	}
	sort.Strings(out)
	return out
}

func (b *Broker) queueLocked(vh, name string, create bool) *queue {
	q := b.queues[vh+"|"+name]
	if q == nil && create {
		q = &queue{name: name, vhost: vh}
		b.queues[vh+"|"+name] = q
		// Every queue is bound to the default exchange by its name.
	}
	return q
}

// routeLocked delivers m to every matching queue and returns how many.
func (b *Broker) routeLocked(vh string, m Message) int {
	e := b.exchanges[vh+"|"+m.Exchange]
	if e == nil {
		return -1
	}
	var targets []*queue
	if m.Exchange == "" {
		if q := b.queues[vh+"|"+m.RoutingKey]; q != nil {
			targets = append(targets, q)
		}
	} else {
		seen := map[string]bool{}
		for _, bd := range e.bindings {
			if seen[bd.queue] {
				continue
			}
			ok := false
			switch e.kind {
			case "fanout":
				ok = true
			case "direct":
				ok = bd.key == m.RoutingKey
			case "topic":
				ok = topicMatch(strings.Split(bd.key, "."), strings.Split(m.RoutingKey, "."))
			}
			if ok {
				if q := b.queues[vh+"|"+bd.queue]; q != nil {
					targets = append(targets, q)
					seen[bd.queue] = true
				}
			}
		}
	}
	for _, q := range targets {
		q.ready = append(q.ready, m)
		b.dispatchLocked(q)
	}
	return len(targets)
}

// topicMatch is RabbitMQ's topic matching: * one word, # zero or more.
func topicMatch(p, k []string) bool {
	if len(p) == 0 {
		return len(k) == 0
	}
	if p[0] == "#" {
		for i := 0; i <= len(k); i++ {
			if topicMatch(p[1:], k[i:]) {
				return true
			}
		}
		return false
	}
	if len(k) == 0 {
		return false
	}
	if p[0] != "*" && p[0] != k[0] {
		return false
	}
	return topicMatch(p[1:], k[1:])
}

// dispatchLocked hands ready messages to consumers with room in their
// channel's prefetch window, round robin.
func (b *Broker) dispatchLocked(q *queue) {
	for len(q.ready) > 0 && len(q.consumers) > 0 {
		delivered := false
		for i := 0; i < len(q.consumers); i++ {
			c := q.consumers[(q.next+i)%len(q.consumers)]
			if !c.noAck && c.ch.prefetch > 0 && len(c.ch.unacked) >= c.ch.prefetch {
				continue
			}
			q.next = (q.next + i + 1) % len(q.consumers)
			m := q.ready[0]
			q.ready = q.ready[1:]
			c.ch.deliver(c, m)
			delivered = true
			break
		}
		if !delivered {
			return
		}
	}
}

func (b *Broker) accept() {
	defer b.wg.Done()
	for {
		nc, err := b.ln.Accept()
		if err != nil {
			return
		}
		c := &conn{b: b, nc: nc, w: bufio.NewWriter(nc), channels: map[uint16]*channel{}}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			nc.Close()
			return
		}
		b.conns[c] = true
		b.mu.Unlock()
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			c.serve()
			b.mu.Lock()
			c.cleanupLocked()
			delete(b.conns, c)
			b.mu.Unlock()
			nc.Close()
		}()
	}
}

type conn struct {
	b        *Broker
	nc       net.Conn
	wmu      sync.Mutex
	w        *bufio.Writer
	vhost    string
	channels map[uint16]*channel
	frameMax int
}

type channel struct {
	c        *conn
	id       uint16
	prefetch int
	tag      uint64
	unacked  map[uint64]unacked
	confirm  bool
	seq      uint64
	pending  *publish
	nextCons int
}

type unacked struct {
	q *queue
	m Message
}

type publish struct {
	exchange, key string
	mandatory     bool
	size          uint64
	props         props
	body          []byte
	gotHeader     bool
}

type props struct {
	contentType, correlation, replyTo, messageID string
	headers                                      map[string]any
	timestamp                                    time.Time
}

// ---- framing ------------------------------------------------------------

func (c *conn) writeFrame(typ byte, ch uint16, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var h [7]byte
	h[0] = typ
	binary.BigEndian.PutUint16(h[1:], ch)
	binary.BigEndian.PutUint32(h[3:], uint32(len(payload)))
	c.w.Write(h[:])
	c.w.Write(payload)
	c.w.WriteByte(frameEnd)
	return c.w.Flush()
}

func (c *conn) method(ch uint16, class, meth uint16, args []byte) error {
	p := make([]byte, 4, 4+len(args))
	binary.BigEndian.PutUint16(p, class)
	binary.BigEndian.PutUint16(p[2:], meth)
	return c.writeFrame(1, ch, append(p, args...))
}

// sendContent writes a method followed by its content header and body.
func (c *conn) sendContent(ch uint16, class, meth uint16, args []byte, m Message) {
	c.method(ch, class, meth, args)
	var w enc
	w.u16(60)
	w.u16(0)
	w.u64(uint64(len(m.Body)))
	flags := uint16(0)
	var pw enc
	if m.ContentType != "" {
		flags |= 1 << 15
		pw.short(m.ContentType)
	}
	if len(m.Headers) > 0 {
		flags |= 1 << 13
		pw.table(m.Headers)
	}
	if m.Correlation != "" {
		flags |= 1 << 10
		pw.short(m.Correlation)
	}
	if m.ReplyTo != "" {
		flags |= 1 << 9
		pw.short(m.ReplyTo)
	}
	if m.MessageID != "" {
		flags |= 1 << 7
		pw.short(m.MessageID)
	}
	if !m.Timestamp.IsZero() {
		flags |= 1 << 6
		pw.u64(uint64(m.Timestamp.Unix()))
	}
	w.u16(flags)
	w.b = append(w.b, pw.b...)
	c.writeFrame(2, ch, w.b)
	max := c.frameMax - 8
	if max <= 0 {
		max = 128 << 10
	}
	body := m.Body
	for len(body) > 0 {
		n := len(body)
		if n > max {
			n = max
		}
		c.writeFrame(3, ch, body[:n])
		body = body[n:]
	}
}

func (c *conn) serve() {
	r := bufio.NewReader(c.nc)
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return
	}
	if !bytes.Equal(hdr, []byte("AMQP\x00\x00\x09\x01")) {
		c.nc.Write([]byte("AMQP\x00\x00\x09\x01"))
		return
	}
	var w enc
	w.u8(0)
	w.u8(9)
	w.table(map[string]any{"product": "sigma amqp091sim", "version": "1.0",
		"capabilities": map[string]any{"publisher_confirms": true, "basic.nack": true, "consumer_cancel_notify": true,
			"exchange_exchange_bindings": true, "connection.blocked": true, "authentication_failure_close": true}})
	w.long("PLAIN AMQPLAIN")
	w.long("en_US")
	c.method(0, 10, 10, w.b)
	for {
		var fh [7]byte
		if _, err := io.ReadFull(r, fh[:]); err != nil {
			return
		}
		typ := fh[0]
		ch := binary.BigEndian.Uint16(fh[1:])
		size := binary.BigEndian.Uint32(fh[3:])
		if size > 16<<20 {
			return
		}
		payload := make([]byte, size+1)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}
		if payload[size] != frameEnd {
			return
		}
		payload = payload[:size]
		var err error
		switch typ {
		case 1:
			err = c.onMethod(ch, payload)
		case 2:
			err = c.onHeader(ch, payload)
		case 3:
			err = c.onBody(ch, payload)
		case 8: // heartbeat
		default:
			err = fmt.Errorf("frame type %d", typ)
		}
		if err != nil {
			return
		}
	}
}

var errClose = errors.New("connection closed")

func (c *conn) closeConn(code uint16, text string, class, meth uint16) error {
	var w enc
	w.u16(code)
	w.short(text)
	w.u16(class)
	w.u16(meth)
	c.method(0, 10, 50, w.b)
	return errClose
}

func (c *conn) closeChannel(ch uint16, code uint16, text string, class, meth uint16) error {
	var w enc
	w.u16(code)
	w.short(text)
	w.u16(class)
	w.u16(meth)
	c.b.mu.Lock()
	if x := c.channels[ch]; x != nil {
		x.releaseLocked()
		delete(c.channels, ch)
	}
	c.b.mu.Unlock()
	return c.method(ch, 20, 40, w.b)
}

func (c *conn) onMethod(chID uint16, p []byte) error {
	if len(p) < 4 {
		return errors.New("short method")
	}
	class, meth := binary.BigEndian.Uint16(p), binary.BigEndian.Uint16(p[2:])
	d := &dec{b: p[4:]}
	b := c.b
	switch {
	case class == 10 && meth == 11: // start-ok
		d.table()
		mech := d.short()
		resp := d.long()
		d.short()
		user, pass := "", ""
		switch mech {
		case "PLAIN":
			parts := strings.Split(resp, "\x00")
			if len(parts) == 3 {
				user, pass = parts[1], parts[2]
			}
		case "AMQPLAIN":
			t := (&dec{b: append([]byte{0, 0, 0, byte(len(resp))}, resp...)}).table()
			user, _ = t["LOGIN"].(string)
			pass, _ = t["PASSWORD"].(string)
		default:
			return c.closeConn(504, "COMMAND_INVALID - unknown mechanism", 10, 11)
		}
		if pw, ok := b.opts.Users[user]; !ok || pw != pass {
			return c.closeConn(403, "ACCESS_REFUSED - Login was refused using authentication mechanism "+mech, 10, 11)
		}
		var w enc
		w.u16(2047)
		w.u32(131072)
		w.u16(60)
		return c.method(0, 10, 30, w.b)
	case class == 10 && meth == 31: // tune-ok
		d.u16()
		c.frameMax = int(d.u32())
		return nil
	case class == 10 && meth == 40: // open
		vh := d.short()
		ok := false
		for _, v := range b.opts.VHosts {
			if v == vh {
				ok = true
			}
		}
		if !ok {
			return c.closeConn(530, "NOT_ALLOWED - vhost "+vh+" not found", 10, 40)
		}
		c.vhost = vh
		var w enc
		w.short("")
		return c.method(0, 10, 41, w.b)
	case class == 10 && meth == 50: // close
		c.method(0, 10, 51, nil)
		return errClose
	case class == 10 && meth == 51:
		return errClose
	case class == 20 && meth == 10: // channel.open
		b.mu.Lock()
		c.channels[chID] = &channel{c: c, id: chID, unacked: map[uint64]unacked{}}
		b.mu.Unlock()
		var w enc
		w.long("")
		return c.method(chID, 20, 11, w.b)
	case class == 20 && meth == 40: // channel.close
		b.mu.Lock()
		if x := c.channels[chID]; x != nil {
			x.releaseLocked()
			delete(c.channels, chID)
		}
		b.mu.Unlock()
		return c.method(chID, 20, 41, nil)
	case class == 20 && meth == 41:
		return nil
	}
	b.mu.Lock()
	ch := c.channels[chID]
	b.mu.Unlock()
	if ch == nil {
		return c.closeConn(504, "CHANNEL_ERROR - expected 'channel.open'", class, meth)
	}
	switch {
	case class == 40 && meth == 10: // exchange.declare
		d.u16()
		name, kind := d.short(), d.short()
		bits := d.u8()
		passive, noWait := bits&1 != 0, bits&16 != 0
		b.mu.Lock()
		e := b.exchanges[c.vhost+"|"+name]
		if e == nil {
			if passive {
				b.mu.Unlock()
				return c.closeChannel(chID, 404, "NOT_FOUND - no exchange '"+name+"' in vhost '"+c.vhost+"'", 40, 10)
			}
			b.exchanges[c.vhost+"|"+name] = &exchange{name: name, kind: kind}
		} else if !passive && e.kind != kind {
			b.mu.Unlock()
			return c.closeChannel(chID, 406, "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '"+name+"'", 40, 10)
		}
		b.mu.Unlock()
		if !noWait {
			return c.method(chID, 40, 11, nil)
		}
		return nil
	case class == 50 && meth == 10: // queue.declare
		d.u16()
		name := d.short()
		bits := d.u8()
		passive, exclusive, noWait := bits&1 != 0, bits&4 != 0, bits&16 != 0
		b.mu.Lock()
		if name == "" {
			b.nextQ++
			name = fmt.Sprintf("amq.gen-%d", b.nextQ)
		}
		q := b.queueLocked(c.vhost, name, false)
		if q == nil {
			if passive {
				b.mu.Unlock()
				return c.closeChannel(chID, 404, "NOT_FOUND - no queue '"+name+"' in vhost '"+c.vhost+"'", 50, 10)
			}
			q = b.queueLocked(c.vhost, name, true)
			if exclusive {
				q.exclusive = c
			}
		} else if q.exclusive != nil && q.exclusive != c {
			b.mu.Unlock()
			return c.closeChannel(chID, 405, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '"+name+"'", 50, 10)
		}
		n, cons := len(q.ready), len(q.consumers)
		b.mu.Unlock()
		if noWait {
			return nil
		}
		var w enc
		w.short(name)
		w.u32(uint32(n))
		w.u32(uint32(cons))
		return c.method(chID, 50, 11, w.b)
	case class == 50 && meth == 20: // queue.bind
		d.u16()
		qn, en, key := d.short(), d.short(), d.short()
		noWait := d.u8()&1 != 0
		b.mu.Lock()
		e := b.exchanges[c.vhost+"|"+en]
		q := b.queues[c.vhost+"|"+qn]
		if e == nil || q == nil {
			b.mu.Unlock()
			what := "exchange '" + en + "'"
			if q == nil {
				what = "queue '" + qn + "'"
			}
			return c.closeChannel(chID, 404, "NOT_FOUND - no "+what+" in vhost '"+c.vhost+"'", 50, 20)
		}
		dup := false
		for _, bd := range e.bindings {
			if bd.queue == qn && bd.key == key {
				dup = true
			}
		}
		if !dup {
			e.bindings = append(e.bindings, binding{queue: qn, key: key})
		}
		b.mu.Unlock()
		if !noWait {
			return c.method(chID, 50, 21, nil)
		}
		return nil
	case class == 60 && meth == 10: // basic.qos
		d.u32()
		n := d.u16()
		b.mu.Lock()
		ch.prefetch = int(n)
		b.mu.Unlock()
		return c.method(chID, 60, 11, nil)
	case class == 60 && meth == 20: // basic.consume
		d.u16()
		qn, tag := d.short(), d.short()
		bits := d.u8()
		noAck, noWait := bits&2 != 0, bits&8 != 0
		b.mu.Lock()
		q := b.queues[c.vhost+"|"+qn]
		if q == nil {
			b.mu.Unlock()
			return c.closeChannel(chID, 404, "NOT_FOUND - no queue '"+qn+"' in vhost '"+c.vhost+"'", 60, 20)
		}
		if tag == "" {
			ch.nextCons++
			tag = fmt.Sprintf("amq.ctag-%d-%d", chID, ch.nextCons)
		}
		cons := &consumer{tag: tag, ch: ch, q: q, noAck: noAck}
		q.consumers = append(q.consumers, cons)
		if !noWait {
			var w enc
			w.short(tag)
			c.method(chID, 60, 21, w.b)
		}
		b.dispatchLocked(q)
		b.mu.Unlock()
		return nil
	case class == 60 && meth == 30: // basic.cancel
		tag := d.short()
		noWait := d.u8()&1 != 0
		b.mu.Lock()
		for _, q := range b.queues {
			for i, x := range q.consumers {
				if x.tag == tag && x.ch == ch {
					q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
					break
				}
			}
		}
		b.mu.Unlock()
		if !noWait {
			var w enc
			w.short(tag)
			return c.method(chID, 60, 31, w.b)
		}
		return nil
	case class == 60 && meth == 40: // basic.publish
		d.u16()
		ex, key := d.short(), d.short()
		bits := d.u8()
		ch.pending = &publish{exchange: ex, key: key, mandatory: bits&1 != 0}
		return nil
	case class == 60 && meth == 80: // basic.ack
		tag := d.u64()
		multiple := d.u8()&1 != 0
		return c.settle(ch, tag, multiple, false)
	case class == 60 && meth == 90: // basic.reject
		tag := d.u64()
		requeue := d.u8()&1 != 0
		return c.settleNack(ch, tag, false, requeue)
	case class == 60 && meth == 120: // basic.nack
		tag := d.u64()
		bits := d.u8()
		return c.settleNack(ch, tag, bits&1 != 0, bits&2 != 0)
	case class == 85 && meth == 10: // confirm.select
		noWait := d.u8()&1 != 0
		b.mu.Lock()
		ch.confirm = true
		b.mu.Unlock()
		if !noWait {
			return c.method(chID, 85, 11, nil)
		}
		return nil
	}
	return c.closeConn(540, fmt.Sprintf("NOT_IMPLEMENTED - method %d.%d", class, meth), class, meth)
}

func (c *conn) onHeader(chID uint16, p []byte) error {
	c.b.mu.Lock()
	ch := c.channels[chID]
	c.b.mu.Unlock()
	if ch == nil || ch.pending == nil {
		return c.closeConn(505, "UNEXPECTED_FRAME - content header without basic.publish", 60, 40)
	}
	d := &dec{b: p}
	d.u16()
	d.u16()
	ch.pending.size = d.u64()
	flags := d.u16()
	pr := props{}
	if flags&(1<<15) != 0 {
		pr.contentType = d.short()
	}
	if flags&(1<<14) != 0 {
		d.short()
	}
	if flags&(1<<13) != 0 {
		pr.headers = d.table()
	}
	if flags&(1<<12) != 0 {
		d.u8()
	}
	if flags&(1<<11) != 0 {
		d.u8()
	}
	if flags&(1<<10) != 0 {
		pr.correlation = d.short()
	}
	if flags&(1<<9) != 0 {
		pr.replyTo = d.short()
	}
	if flags&(1<<8) != 0 {
		d.short()
	}
	if flags&(1<<7) != 0 {
		pr.messageID = d.short()
	}
	if flags&(1<<6) != 0 {
		pr.timestamp = time.Unix(int64(d.u64()), 0)
	}
	ch.pending.props, ch.pending.gotHeader = pr, true
	if ch.pending.size == 0 {
		return c.finishPublish(ch)
	}
	return nil
}

func (c *conn) onBody(chID uint16, p []byte) error {
	c.b.mu.Lock()
	ch := c.channels[chID]
	c.b.mu.Unlock()
	if ch == nil || ch.pending == nil || !ch.pending.gotHeader {
		return c.closeConn(505, "UNEXPECTED_FRAME - body without header", 60, 40)
	}
	ch.pending.body = append(ch.pending.body, p...)
	if uint64(len(ch.pending.body)) >= ch.pending.size {
		return c.finishPublish(ch)
	}
	return nil
}

func (c *conn) finishPublish(ch *channel) error {
	p := ch.pending
	ch.pending = nil
	m := Message{Exchange: p.exchange, RoutingKey: p.key, Body: p.body, Headers: p.props.headers,
		ContentType: p.props.contentType, Correlation: p.props.correlation, ReplyTo: p.props.replyTo,
		MessageID: p.props.messageID, Timestamp: p.props.timestamp}
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	b := c.b
	b.mu.Lock()
	b.published = append(b.published, m)
	n := b.routeLocked(c.vhost, m)
	confirm := ch.confirm
	if confirm {
		ch.seq++
	}
	seq := ch.seq
	b.mu.Unlock()
	if n < 0 {
		return c.closeChannel(ch.id, 404, "NOT_FOUND - no exchange '"+p.exchange+"' in vhost '"+c.vhost+"'", 60, 40)
	}
	if n == 0 && p.mandatory {
		var w enc
		w.u16(312)
		w.short("NO_ROUTE")
		w.short(p.exchange)
		w.short(p.key)
		c.sendContent(ch.id, 60, 50, w.b, m)
	}
	if confirm {
		var w enc
		w.u64(seq)
		w.u8(0)
		return c.method(ch.id, 60, 80, w.b)
	}
	return nil
}

func (ch *channel) deliver(cons *consumer, m Message) {
	ch.tag++
	tag := ch.tag
	if !cons.noAck {
		ch.unacked[tag] = unacked{q: cons.q, m: m}
		cons.q.unacked++
	}
	var w enc
	w.short(cons.tag)
	w.u64(tag)
	if m.Redelivered {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.short(m.Exchange)
	w.short(m.RoutingKey)
	// Written under the broker lock so deliveries keep their order; the
	// connection's own lock keeps frames whole.
	ch.c.sendContent(ch.id, 60, 60, w.b, m)
}

func (c *conn) settle(ch *channel, tag uint64, multiple, _ bool) error {
	b := c.b
	b.mu.Lock()
	defer b.mu.Unlock()
	var tags []uint64
	if multiple {
		for t := range ch.unacked {
			if t <= tag {
				tags = append(tags, t)
			}
		}
	} else if _, ok := ch.unacked[tag]; ok {
		tags = []uint64{tag}
	} else {
		go c.closeChannel(ch.id, 406, fmt.Sprintf("PRECONDITION_FAILED - unknown delivery tag %d", tag), 60, 80)
		return nil
	}
	qs := map[*queue]bool{}
	for _, t := range tags {
		u := ch.unacked[t]
		delete(ch.unacked, t)
		u.q.unacked--
		qs[u.q] = true
	}
	for q := range qs {
		b.dispatchLocked(q)
	}
	return nil
}

func (c *conn) settleNack(ch *channel, tag uint64, multiple, requeue bool) error {
	b := c.b
	b.mu.Lock()
	defer b.mu.Unlock()
	var tags []uint64
	for t := range ch.unacked {
		if t == tag || (multiple && t <= tag) {
			tags = append(tags, t)
		}
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	qs := map[*queue]bool{}
	for _, t := range tags {
		u := ch.unacked[t]
		delete(ch.unacked, t)
		u.q.unacked--
		if requeue {
			u.m.Redelivered = true
			u.q.ready = append([]Message{u.m}, u.q.ready...)
		}
		qs[u.q] = true
	}
	for q := range qs {
		b.dispatchLocked(q)
	}
	return nil
}

// releaseLocked returns a closing channel's unacknowledged messages to
// their queues, marked redelivered, and removes its consumers.
func (ch *channel) releaseLocked() {
	tags := make([]uint64, 0, len(ch.unacked))
	for t := range ch.unacked {
		tags = append(tags, t)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] > tags[j] })
	qs := map[*queue]bool{}
	for _, t := range tags {
		u := ch.unacked[t]
		u.m.Redelivered = true
		u.q.ready = append([]Message{u.m}, u.q.ready...)
		u.q.unacked--
		qs[u.q] = true
	}
	ch.unacked = map[uint64]unacked{}
	for _, q := range ch.c.b.queues {
		kept := q.consumers[:0]
		for _, x := range q.consumers {
			if x.ch != ch {
				kept = append(kept, x)
			}
		}
		q.consumers = kept
	}
	for q := range qs {
		ch.c.b.dispatchLocked(q)
	}
}

func (c *conn) cleanupLocked() {
	for _, ch := range c.channels {
		ch.releaseLocked()
	}
	c.channels = map[uint16]*channel{}
	for k, q := range c.b.queues {
		if q.exclusive == c {
			delete(c.b.queues, k)
		}
	}
}

// ---- encoding -----------------------------------------------------------

type enc struct{ b []byte }

func (e *enc) u8(v uint8)   { e.b = append(e.b, v) }
func (e *enc) u16(v uint16) { e.b = binary.BigEndian.AppendUint16(e.b, v) }
func (e *enc) u32(v uint32) { e.b = binary.BigEndian.AppendUint32(e.b, v) }
func (e *enc) u64(v uint64) { e.b = binary.BigEndian.AppendUint64(e.b, v) }
func (e *enc) short(s string) {
	if len(s) > 255 {
		s = s[:255]
	}
	e.u8(uint8(len(s)))
	e.b = append(e.b, s...)
}
func (e *enc) long(s string) { e.u32(uint32(len(s))); e.b = append(e.b, s...) }

func (e *enc) table(t map[string]any) {
	var inner enc
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		inner.short(k)
		inner.value(t[k])
	}
	e.u32(uint32(len(inner.b)))
	e.b = append(e.b, inner.b...)
}

func (e *enc) value(v any) {
	switch x := v.(type) {
	case bool:
		e.u8('t')
		if x {
			e.u8(1)
		} else {
			e.u8(0)
		}
	case string:
		e.u8('S')
		e.long(x)
	case []byte:
		e.u8('x')
		e.u32(uint32(len(x)))
		e.b = append(e.b, x...)
	case int:
		e.u8('l')
		e.u64(uint64(x))
	case int32:
		e.u8('I')
		e.u32(uint32(x))
	case int64:
		e.u8('l')
		e.u64(uint64(x))
	case map[string]any:
		e.u8('F')
		e.table(x)
	case nil:
		e.u8('V')
	default:
		e.u8('S')
		e.long(fmt.Sprint(x))
	}
}

type dec struct {
	b   []byte
	err bool
}

func (d *dec) take(n int) []byte {
	if len(d.b) < n {
		d.err = true
		d.b = nil
		return make([]byte, n)
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}
func (d *dec) u8() uint8     { return d.take(1)[0] }
func (d *dec) u16() uint16   { return binary.BigEndian.Uint16(d.take(2)) }
func (d *dec) u32() uint32   { return binary.BigEndian.Uint32(d.take(4)) }
func (d *dec) u64() uint64   { return binary.BigEndian.Uint64(d.take(8)) }
func (d *dec) short() string { return string(d.take(int(d.u8()))) }
func (d *dec) long() string  { return string(d.take(int(d.u32()))) }

func (d *dec) table() map[string]any {
	n := d.u32()
	sub := &dec{b: d.take(int(n))}
	out := map[string]any{}
	for len(sub.b) > 0 && !sub.err {
		k := sub.short()
		out[k] = sub.value()
	}
	return out
}

func (d *dec) value() any {
	switch d.u8() {
	case 't':
		return d.u8() != 0
	case 'b', 'B':
		return int64(d.u8())
	case 's', 'u':
		return int64(d.u16())
	case 'I', 'i':
		return int64(int32(d.u32()))
	case 'l', 'L':
		return int64(d.u64())
	case 'f':
		d.u32()
		return nil
	case 'd':
		d.u64()
		return nil
	case 'D':
		d.u8()
		d.u32()
		return nil
	case 'S':
		return d.long()
	case 'x':
		return d.take(int(d.u32()))
	case 'A':
		n := d.u32()
		sub := &dec{b: d.take(int(n))}
		var out []any
		for len(sub.b) > 0 && !sub.err {
			out = append(out, sub.value())
		}
		return out
	case 'T':
		return time.Unix(int64(d.u64()), 0)
	case 'F':
		return d.table()
	case 'V':
		return nil
	}
	d.err = true
	d.b = nil
	return nil
}

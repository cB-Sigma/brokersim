// Package amqpsim is an in-process AMQP 1.0 broker for tests, built from
// the OASIS AMQP 1.0 specification (frames, performatives, SASL, link
// credit, settlement) and the public behaviour of Azure Service Bus and
// Event Hubs:
//
//   - queues (Service Bus, ActiveMQ Artemis anycast): a message is taken
//     off the queue when the receiver accepts it, and goes back (with its
//     delivery count raised) when the receiver releases or modifies it or
//     its link or connection ends before it settles;
//   - event hubs: a partitioned log read at <hub>/ConsumerGroups/<group>/
//     Partitions/<n>, each event carrying x-opt-offset,
//     x-opt-sequence-number and x-opt-enqueued-time annotations; a
//     receiver positions itself with the apache.org:selector-filter:string
//     filter on those annotations, and settling an event does not remove
//     it;
//   - the $management node (READ of com.microsoft:eventhub answers the
//     partition IDs) and the $cbs node (put-token, checked by a hook);
//   - SASL PLAIN and ANONYMOUS, an unknown address refused with
//     amqp:not-found, a message refused by a hook with amqp:rejected;
//   - since v0.2.0 (servicebus.go): lock tokens and sequence numbers,
//     abandon, defer and dead-letter outcomes with a $DeadLetterQueue
//     subqueue and a maximum delivery count, scheduled messages, the
//     Service Bus $management operations (schedule, cancel, lock
//     renewal), message sessions, Event Hubs batches and partition
//     properties, TLS, throttling and size refusals.
//
// References: OASIS AMQP 1.0 (http://docs.oasis-open.org/amqp/core/v1.0/),
// Event Hubs AMQP (https://learn.microsoft.com/azure/event-hubs/
// event-hubs-amqp-troubleshoot and the AMQP 1.0 in Service Bus and Event
// Hubs protocol guide), AMQP Claims-Based Security draft.
package amqpsim

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// Options configure the broker.
type Options struct {
	// Users enables SASL PLAIN with these user names and passwords;
	// Anonymous also allows SASL ANONYMOUS. Neither: no SASL needed.
	Users     map[string]string
	Anonymous bool
	// AutoCreate creates a queue the first time an unknown address is
	// attached, as Artemis does; otherwise it is refused as Service Bus
	// refuses it.
	AutoCreate bool
	// Reject, when set, decides whether a message sent to address is
	// refused (amqp:rejected with the error's text).
	Reject func(address string, m *amqp.Message) error
	// PutToken checks a CBS put-token (audience, token); nil accepts
	// every token.
	PutToken func(audience, token string) bool
	// TLS serves amqps with this configuration (testcert makes one).
	TLS *tls.Config
	// Throttle, when set, is asked before a message sent to address is
	// taken; an error refuses it with com.microsoft:server-busy (as a
	// throttled namespace does) and the error's text.
	Throttle func(address string) error
	// MaxSize refuses a message larger than this many bytes with
	// amqp:link:message-size-exceeded (the quota of a namespace tier);
	// 0 is no limit.
	MaxSize int
}

// Received is a message a client sent to an address.
type Received struct {
	Address string
	Message *amqp.Message
}

// Broker is the in-process broker.
type Broker struct {
	opts Options
	ln   net.Listener

	mu       sync.Mutex
	queues   map[string]*queue
	hubs     map[string]*hub
	received []Received
	conns    map[*conn]bool
	closed   bool
	wg       sync.WaitGroup
	tokens   []string
}

type stored struct {
	payload []byte
	msg     *amqp.Message
	count   uint32 // deliveries so far

	// Service Bus queues
	seq       int64
	enqueued  time.Time
	visibleAt time.Time // scheduled messages
	session   string    // the message's group ID
}

type queue struct {
	name     string
	ready    []*stored
	opts     QueueOptions
	nextSeq  int64
	dlq      *queue
	locks    map[string]*link // session ID → the link holding its lock
	deferred []*stored
}

type hub struct {
	name       string
	partitions [][]*event
}

type event struct {
	stored
	offset   int64
	seq      int64
	enqueued time.Time
}

// Start starts a broker on 127.0.0.1 and an ephemeral port.
func Start(o Options) (*Broker, error) { return StartOn("127.0.0.1:0", o) }

// StartOn starts a broker listening on addr (host:port).
func StartOn(addr string, o Options) (*Broker, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if o.TLS != nil {
		ln = tls.NewListener(ln, o.TLS)
	}
	b := &Broker{opts: o, ln: ln, queues: map[string]*queue{}, hubs: map[string]*hub{}, conns: map[*conn]bool{}}
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

// DropConnections closes every client connection.
func (b *Broker) DropConnections() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.conns {
		c.nc.Close()
	}
}

// CreateQueue creates a queue.
func (b *Broker) CreateQueue(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queues[name] == nil {
		b.queues[name] = &queue{name: name}
	}
}

// CreateEventHub creates an event hub with n partitions ("0" … "n-1").
func (b *Broker) CreateEventHub(name string, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hubs[name] == nil {
		b.hubs[name] = &hub{name: name, partitions: make([][]*event, n)}
	}
}

// Send puts a message on a queue, as another client's sender would.
func (b *Broker) Send(queueName string, m *amqp.Message) error {
	payload, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = payload
	q := b.queueFor(queueName)
	if q == nil {
		return fmt.Errorf("no queue %s", queueName)
	}
	b.enqueueLocked(q, m)
	b.pumpLocked()
	return nil
}

// SendEvent appends an event to a hub's partition, stamped with its
// offset, sequence number and enqueued time; annotations are added (an
// IoT hub's iothub-connection-device-id).
func (b *Broker) SendEvent(hubName string, partition int, m *amqp.Message, annotations map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := b.hubs[hubName]
	if h == nil || partition < 0 || partition >= len(h.partitions) {
		return fmt.Errorf("no partition %d in event hub %s", partition, hubName)
	}
	b.appendEventLocked(h, partition, m, annotations)
	b.pumpLocked()
	return nil
}

func (b *Broker) appendEventLocked(h *hub, p int, m *amqp.Message, annotations map[string]any) {
	part := h.partitions[p]
	ev := &event{seq: int64(len(part)), enqueued: time.Now().UTC().Truncate(time.Millisecond)}
	ev.offset = 0
	if len(part) > 0 {
		last := part[len(part)-1]
		ev.offset = last.offset + int64(len(last.payload)) + 64
	}
	cp := *m
	if cp.Annotations == nil {
		cp.Annotations = amqp.Annotations{}
	} else {
		a := amqp.Annotations{}
		for k, v := range cp.Annotations {
			a[k] = v
		}
		cp.Annotations = a
	}
	for k, v := range annotations {
		cp.Annotations[amqp.Symbol(k)] = v
	}
	cp.Annotations[amqp.Symbol("x-opt-offset")] = strconv.FormatInt(ev.offset, 10)
	cp.Annotations[amqp.Symbol("x-opt-sequence-number")] = ev.seq
	cp.Annotations[amqp.Symbol("x-opt-enqueued-time")] = ev.enqueued
	payload, _ := cp.MarshalBinary()
	ev.payload, ev.msg = payload, &cp
	h.partitions[p] = append(part, ev)
}

// Received returns what clients sent, in order.
func (b *Broker) Received() []Received {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Received(nil), b.received...)
}

// Depth returns a queue's ready and in-flight (delivered, unsettled)
// message counts.
func (b *Broker) Depth(queueName string) (ready, inflight int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if q := b.queues[queueName]; q != nil {
		ready = len(q.ready)
	}
	for c := range b.conns {
		for _, s := range c.sessions {
			for _, l := range s.links {
				if l.q != nil && l.q.name == queueName {
					inflight += len(l.unsettled)
				}
			}
		}
	}
	return
}

// Tokens returns the CBS tokens put, in order.
func (b *Broker) Tokens() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.tokens...)
}

func (b *Broker) accept() {
	defer b.wg.Done()
	for {
		nc, err := b.ln.Accept()
		if err != nil {
			return
		}
		c := &conn{b: b, nc: nc, w: bufio.NewWriter(nc), sessions: map[uint16]*session{}}
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
			err := c.serve()
			_ = err
			b.mu.Lock()
			c.releaseLocked()
			delete(b.conns, c)
			b.pumpLocked()
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
	sessions map[uint16]*session
	maxFrame uint32
}

type session struct {
	ch             uint16
	nextDelivery   uint32
	links          map[uint32]*link // by the client's handle
	nextHandle     uint32
	nextIncomingID uint32
}

type link struct {
	name       string
	handle     uint32 // client's handle
	ours       uint32
	sender     bool // the broker sends on it (a client receiver)
	address    string
	q          *queue
	hub        *hub
	partition  int
	pos        int
	credit     uint32
	count      uint32 // delivery-count
	unsettled  map[uint32]*stored
	pending    []*stored // replies for a $management or $cbs reply link
	replyTo    string
	inbound    []byte // a transfer still arriving
	inboundID  uint32
	inboundSet bool
	settledIn  bool
	format     uint32 // message format of the transfer arriving
	session    string // the Service Bus session this receiver holds
	entity     string // a  link's entity ("" for the namespace)
}

// ---- frames -------------------------------------------------------------

func (c *conn) frame(typ byte, ch uint16, perf func(*writer), payload []byte) error {
	var body writer
	perf(&body)
	body.b = append(body.b, payload...)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var h [8]byte
	binary.BigEndian.PutUint32(h[:], uint32(8+len(body.b)))
	h[4], h[5] = 2, typ
	binary.BigEndian.PutUint16(h[6:], ch)
	c.w.Write(h[:])
	c.w.Write(body.b)
	return c.w.Flush()
}

func readFrame(r io.Reader) (typ byte, ch uint16, body []byte, err error) {
	var h [8]byte
	if _, err = io.ReadFull(r, h[:]); err != nil {
		return
	}
	size := binary.BigEndian.Uint32(h[:])
	doff := int(h[4]) * 4
	if size < 8 || size > 16<<20 || doff < 8 || int(size) < doff {
		return 0, 0, nil, errBadFrame
	}
	rest := make([]byte, size-8)
	if _, err = io.ReadFull(r, rest); err != nil {
		return
	}
	return h[5], binary.BigEndian.Uint16(h[6:]), rest[doff-8:], nil
}

func (c *conn) serve() error {
	r := bufio.NewReader(c.nc)
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return err
	}
	if string(hdr[:4]) != "AMQP" {
		return errBadFrame
	}
	needSASL := len(c.b.opts.Users) > 0 || c.b.opts.Anonymous
	if hdr[4] == 3 {
		c.nc.Write([]byte("AMQP\x03\x01\x00\x00"))
		if err := c.sasl(r); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, hdr); err != nil {
			return err
		}
	} else if needSASL {
		c.nc.Write([]byte("AMQP\x03\x01\x00\x00"))
		return errors.New("SASL required")
	}
	if hdr[4] != 0 {
		return errBadFrame
	}
	c.nc.Write([]byte("AMQP\x00\x01\x00\x00"))
	for {
		typ, ch, body, err := readFrame(r)
		if err != nil {
			return err
		}
		if len(body) == 0 || typ != 0 {
			continue // heartbeat
		}
		code, f, payload, err := performative(body)
		if err != nil {
			return err
		}
		if err := c.onFrame(ch, code, f, payload); err != nil {
			return err
		}
	}
}

func (c *conn) sasl(r io.Reader) error {
	mechs := []string{}
	if len(c.b.opts.Users) > 0 {
		mechs = append(mechs, "PLAIN")
	}
	if c.b.opts.Anonymous || len(c.b.opts.Users) == 0 {
		mechs = append(mechs, "ANONYMOUS")
	}
	c.frame(1, 0, func(w *writer) {
		w.described(codeSASLMechs, func(w *writer) { w.list(func(w *writer) { w.symArray(mechs...) }) })
	}, nil)
	_, _, body, err := readFrame(r)
	if err != nil {
		return err
	}
	code, f, _, err := performative(body)
	if err != nil || code != codeSASLInit {
		return errBadFrame
	}
	mech := asString(field(f, 0))
	resp, _ := field(f, 1).([]byte)
	ok := false
	switch mech {
	case "PLAIN":
		parts := strings.Split(string(resp), "\x00")
		if len(parts) == 3 {
			pw, known := c.b.opts.Users[parts[1]]
			ok = known && pw == parts[2]
		}
	case "ANONYMOUS":
		ok = c.b.opts.Anonymous || len(c.b.opts.Users) == 0
	}
	outcome := uint8(0)
	if !ok {
		outcome = 1 // auth
	}
	c.frame(1, 0, func(w *writer) {
		w.described(codeSASLOutcome, func(w *writer) { w.list(func(w *writer) { w.ubyte(outcome) }) })
	}, nil)
	if !ok {
		return errors.New("authentication failed")
	}
	return nil
}

func (c *conn) onFrame(ch uint16, code uint64, f []any, payload []byte) error {
	b := c.b
	switch code {
	case codeOpen:
		if v, ok := asUint(field(f, 2)); ok {
			c.maxFrame = uint32(v)
		}
		return c.frame(0, 0, func(w *writer) {
			w.described(codeOpen, func(w *writer) {
				w.list(func(w *writer) { w.str("amqpsim") }, func(w *writer) { w.null() },
					func(w *writer) { w.uint(65536) }, func(w *writer) { w.ushort(255) })
			})
		}, nil)
	case codeBegin:
		next, _ := asUint(field(f, 1))
		b.mu.Lock()
		c.sessions[ch] = &session{ch: ch, links: map[uint32]*link{}, nextIncomingID: uint32(next)}
		b.mu.Unlock()
		return c.frame(0, ch, func(w *writer) {
			w.described(codeBegin, func(w *writer) {
				w.list(func(w *writer) { w.ushort(ch) }, func(w *writer) { w.uint(0) },
					func(w *writer) { w.uint(65535) }, func(w *writer) { w.uint(65535) }, func(w *writer) { w.uint(1023) })
			})
		}, nil)
	case codeEnd:
		b.mu.Lock()
		if s := c.sessions[ch]; s != nil {
			for _, l := range s.links {
				b.releaseLinkLocked(l)
			}
			delete(c.sessions, ch)
		}
		b.pumpLocked()
		b.mu.Unlock()
		return c.frame(0, ch, func(w *writer) { w.described(codeEnd, func(w *writer) { w.list() }) }, nil)
	case codeClose:
		c.frame(0, 0, func(w *writer) { w.described(codeClose, func(w *writer) { w.list() }) }, nil)
		return io.EOF
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := c.sessions[ch]
	if s == nil {
		return errBadFrame
	}
	switch code {
	case codeAttach:
		return c.attachLocked(s, f)
	case codeFlow:
		h, ok := asUint(field(f, 4))
		if !ok {
			return nil // session flow only
		}
		l := s.links[uint32(h)]
		if l == nil || !l.sender {
			return nil
		}
		rcvCount, _ := asUint(field(f, 5))
		credit, _ := asUint(field(f, 6))
		if field(f, 5) == nil {
			rcvCount = uint64(l.count)
		}
		n := int64(rcvCount) + int64(credit) - int64(l.count)
		if n < 0 {
			n = 0
		}
		l.credit = uint32(n)
		b.pumpLocked()
		return nil
	case codeTransfer:
		h, _ := asUint(field(f, 0))
		l := s.links[uint32(h)]
		if l == nil || l.sender {
			return errBadFrame
		}
		if !l.inboundSet {
			id, _ := asUint(field(f, 1))
			l.inboundID, l.inboundSet = uint32(id), true
			l.settledIn = asBool(field(f, 4))
			fmtv, _ := asUint(field(f, 3))
			l.format = uint32(fmtv)
		}
		l.inbound = append(l.inbound, payload...)
		if asBool(field(f, 5)) { // more
			return nil
		}
		data, id, settled := l.inbound, l.inboundID, l.settledIn
		l.inbound, l.inboundSet = nil, false
		s.nextIncomingID = id + 1
		var m amqp.Message
		if err := m.UnmarshalBinary(data); err != nil {
			return err
		}
		rejectErr := b.deliverInboundLocked(c, s, l, &m, data)
		if l.credit > 0 {
			l.credit--
		}
		l.count++
		if l.credit < 100 {
			l.credit = 1000
			c.flowLocked(s, l)
		}
		if settled {
			return nil
		}
		return c.frame(0, s.ch, func(w *writer) {
			w.described(codeDisposition, func(w *writer) {
				w.list(func(w *writer) { w.boolean(true) }, func(w *writer) { w.uint(id) }, func(w *writer) { w.uint(id) },
					func(w *writer) { w.boolean(true) }, func(w *writer) {
						if rejectErr != nil {
							w.described(codeRejected, func(w *writer) {
								cond := "amqp:rejected"
								var ce *condError
								if errors.As(rejectErr, &ce) {
									cond = ce.cond
								}
								w.list(errorValue(cond, rejectErr.Error()))
							})
						} else {
							w.described(codeAccepted, func(w *writer) { w.list() })
						}
					})
			})
		}, nil)
	case codeDisposition:
		if !asBool(field(f, 0)) { // role sender: about the client's own transfers
			return nil
		}
		first, _ := asUint(field(f, 1))
		last := first
		if v, ok := asUint(field(f, 2)); ok {
			last = v
		}
		state, _ := field(f, 4).(Described)
		for id := uint32(first); id <= uint32(last); id++ {
			for _, l := range s.links {
				st := l.unsettled[id]
				if st == nil {
					continue
				}
				delete(l.unsettled, id)
				if l.q != nil {
					b.settleQueueLocked(l.q, st, state)
				}
			}
			if id == uint32(last) {
				break
			}
		}
		b.pumpLocked()
		if !asBool(field(f, 3)) {
			// Receiver settle mode second: the client waits for the
			// broker to settle the outcome.
			return c.frame(0, s.ch, func(w *writer) {
				w.described(codeDisposition, func(w *writer) {
					w.list(func(w *writer) { w.boolean(false) }, func(w *writer) { w.uint(uint32(first)) }, func(w *writer) { w.uint(uint32(last)) },
						func(w *writer) { w.boolean(true) }, func(w *writer) {
							if state.Code == codeRejected {
								// The outcome is taken: settled as rejected,
								// without the client's error (which clients
								// would read as a failure of the settlement).
								w.described(codeRejected, func(w *writer) { w.list() })
								return
							}
							w.any(field(f, 4))
						})
				})
			}, nil)
		}
		return nil
	case codeDetach:
		h, _ := asUint(field(f, 0))
		l := s.links[uint32(h)]
		if l != nil {
			b.releaseLinkLocked(l)
			delete(s.links, uint32(h))
			b.pumpLocked()
			return c.frame(0, s.ch, func(w *writer) {
				w.described(codeDetach, func(w *writer) {
					w.list(func(w *writer) { w.uint(l.ours) }, func(w *writer) { w.boolean(true) })
				})
			}, nil)
		}
		return nil
	}
	return nil
}

func (c *conn) flowLocked(s *session, l *link) {
	c.frame(0, s.ch, func(w *writer) {
		w.described(codeFlow, func(w *writer) {
			w.list(func(w *writer) { w.uint(s.nextIncomingID) }, func(w *writer) { w.uint(65535) },
				func(w *writer) { w.uint(s.nextDelivery) }, func(w *writer) { w.uint(65535) },
				func(w *writer) { w.uint(l.ours) }, func(w *writer) { w.uint(l.count) }, func(w *writer) { w.uint(l.credit) })
		})
	}, nil)
}

var hubAddr = regexp.MustCompile(`^([^/]+)/ConsumerGroups/([^/]+)/Partitions/(\d+)$`)
var hubSend = regexp.MustCompile(`^([^/]+)(/Partitions/(\d+))?$`)

func (c *conn) attachLocked(s *session, f []any) error {
	b := c.b
	name := asString(field(f, 0))
	h, _ := asUint(field(f, 1))
	clientReceiver := asBool(field(f, 2))
	src, _ := field(f, 5).(Described)
	tgt, _ := field(f, 6).(Described)
	srcL, _ := src.Value.([]any)
	tgtL, _ := tgt.Value.([]any)
	l := &link{name: name, handle: uint32(h), ours: s.nextHandle, sender: clientReceiver, unsettled: map[uint32]*stored{}, partition: -1}
	s.nextHandle++
	refuse, refuseCond, sessionLocked := "", "amqp:not-found", false
	if clientReceiver {
		l.address = asString(field(srcL, 0))
		l.replyTo = asString(field(tgtL, 0))
		switch {
		case isNode(l.address):
		case hubAddr.MatchString(l.address):
			m := hubAddr.FindStringSubmatch(l.address)
			hb := b.hubs[m[1]]
			p, _ := strconv.Atoi(m[3])
			if hb == nil || p >= len(hb.partitions) {
				refuse = "The messaging entity '" + l.address + "' could not be found."
				break
			}
			l.hub, l.partition = hb, p
			l.pos = len(hb.partitions[p]) // default: new events only
			if filters, ok := field(srcL, 7).(map[any]any); ok {
				for _, v := range filters {
					if d, ok := v.(Described); ok {
						l.pos = startPosition(hb.partitions[p], asString(d.Value))
					}
				}
			}
		default:
			q := b.queueFor(l.address)
			if q == nil && b.opts.AutoCreate {
				q = &queue{name: l.address}
				b.queues[l.address] = q
			}
			if q == nil {
				refuse = "The messaging entity '" + l.address + "' could not be found."
			}
			l.q = q
			if q != nil {
				if filters, ok := field(srcL, 7).(map[any]any); ok {
					for k, v := range filters {
						if asString(k) != "com.microsoft:session-filter" {
							continue
						}
						d, _ := v.(Described)
						sid, why := q.lockSessionLocked(l, asString(d.Value), d.Value == nil)
						if why != "" {
							refuse, refuseCond = why, "com.microsoft:session-cannot-be-locked"
							if d.Value == nil {
								refuseCond = "com.microsoft:timeout"
							}
							break
						}
						d.Value = sid
						filters[k] = d
						sessionLocked = true
					}
				}
			}
		}
	} else {
		l.address = asString(field(tgtL, 0))
		switch {
		case isNode(l.address):
		case b.hubs[strings.SplitN(l.address, "/", 2)[0]] != nil && hubSend.MatchString(l.address):
		default:
			if b.queueFor(l.address) == nil {
				if b.opts.AutoCreate {
					b.queues[l.address] = &queue{name: l.address}
				} else {
					refuse = "The messaging entity '" + l.address + "' could not be found."
				}
			}
		}
	}
	initial, _ := asUint(field(f, 9))
	if !clientReceiver {
		l.count = uint32(initial)
	}
	err := c.frame(0, s.ch, func(w *writer) {
		w.described(codeAttach, func(w *writer) {
			items := []func(*writer){
				func(w *writer) { w.str(name) }, func(w *writer) { w.uint(l.ours) }, func(w *writer) { w.boolean(!clientReceiver) },
				func(w *writer) { w.ubyte(settleMode(field(f, 3), 2)) }, func(w *writer) { w.ubyte(settleMode(field(f, 4), 0)) },
			}
			if refuse != "" {
				items = append(items, func(w *writer) { w.null() }, func(w *writer) { w.null() })
			} else {
				items = append(items, func(w *writer) { w.any(field(f, 5)) }, func(w *writer) { w.any(field(f, 6)) })
			}
			items = append(items, func(w *writer) { w.null() }, func(w *writer) { w.boolean(false) }, func(w *writer) { w.uint(0) }, func(w *writer) { w.ulong(1 << 20) })
			if sessionLocked {
				// offered and desired capabilities, then the session lock's expiry.
				until := dotnetTicks(time.Now().Add(l.q.options().LockDuration))
				items = append(items, func(w *writer) { w.null() }, func(w *writer) { w.null() },
					func(w *writer) {
						w.mapOf(func(w *writer) { w.sym("com.microsoft:locked-until-utc") }, func(w *writer) { w.long(until) })
					})
			}
			w.list(items...)
		})
	}, nil)
	if err != nil {
		return err
	}
	if refuse != "" {
		return c.frame(0, s.ch, func(w *writer) {
			w.described(codeDetach, func(w *writer) {
				w.list(func(w *writer) { w.uint(l.ours) }, func(w *writer) { w.boolean(true) }, errorValue(refuseCond, refuse))
			})
		}, nil)
	}
	s.links[l.handle] = l
	if !clientReceiver {
		l.credit = 1000
		c.flowLocked(s, l)
	}
	b.pumpLocked()
	return nil
}

func settleMode(v any, d uint8) uint8 {
	if n, ok := asUint(v); ok {
		return uint8(n)
	}
	return d
}

var selectorRE = regexp.MustCompile(`amqp\.annotation\.(x-opt-offset|x-opt-sequence-number|x-opt-enqueued-time)\s*(>=|>|=)\s*'?(-?\d+)'?`)

// startPosition reads an Event Hubs selector filter and returns the first
// event index it lets through.
func startPosition(evs []*event, filter string) int {
	m := selectorRE.FindStringSubmatch(filter)
	if m == nil {
		return len(evs)
	}
	n, _ := strconv.ParseInt(m[3], 10, 64)
	for i, ev := range evs {
		var v int64
		switch m[1] {
		case "x-opt-offset":
			v = ev.offset
		case "x-opt-sequence-number":
			v = ev.seq
		default:
			v = ev.enqueued.UnixMilli()
		}
		if (m[2] == ">" && v > n) || (m[2] == ">=" && v >= n) || (m[2] == "=" && v == n) {
			return i
		}
	}
	return len(evs)
}

// deliverInboundLocked handles a message a client sent.
func (b *Broker) deliverInboundLocked(c *conn, s *session, l *link, m *amqp.Message, data []byte) error {
	switch {
	case l.address == "$management" || strings.HasSuffix(l.address, "/$management"):
		return b.managementLocked(c, l, m)
	case l.address == "$cbs":
		return b.cbsLocked(c, m)
	}
	if b.opts.MaxSize > 0 && len(data) > b.opts.MaxSize {
		return &condError{"amqp:link:message-size-exceeded", fmt.Sprintf("The received message (%d bytes) exceeds the limit (%d bytes).", len(data), b.opts.MaxSize)}
	}
	if b.opts.Throttle != nil {
		if err := b.opts.Throttle(l.address); err != nil {
			return &condError{"com.microsoft:server-busy", err.Error()}
		}
	}
	msgs := unbatch(l.format, m)
	if b.opts.Reject != nil {
		for _, one := range msgs {
			if err := b.opts.Reject(l.address, one); err != nil {
				return err
			}
		}
	}
	for _, one := range msgs {
		b.received = append(b.received, Received{Address: l.address, Message: one})
	}
	if parts := hubSend.FindStringSubmatch(l.address); parts != nil && b.hubs[parts[1]] != nil {
		h := b.hubs[parts[1]]
		p := 0
		if parts[3] != "" {
			p, _ = strconv.Atoi(parts[3])
		}
		if p < len(h.partitions) {
			for _, one := range msgs {
				b.appendEventLocked(h, p, one, nil)
			}
		}
	} else if q := b.queueFor(l.address); q != nil {
		for _, one := range msgs {
			b.enqueueLocked(q, one)
		}
	}
	b.pumpLocked()
	return nil
}

func (b *Broker) replyLocked(c *conn, req *amqp.Message, status int32, desc string, value any) {
	to := ""
	if req.Properties != nil && req.Properties.ReplyTo != nil {
		to = *req.Properties.ReplyTo
	}
	resp := &amqp.Message{ApplicationProperties: map[string]any{"status-code": status, "statusCode": status, "status-description": desc}, Value: value}
	resp.Properties = &amqp.MessageProperties{To: &to}
	if req.Properties != nil {
		resp.Properties.CorrelationID = req.Properties.MessageID
	}
	payload, _ := resp.MarshalBinary()
	for _, s := range c.sessions {
		for _, l := range s.links {
			if l.sender && isNode(l.address) && (l.replyTo == to || to == "") {
				l.pending = append(l.pending, &stored{payload: payload, msg: resp})
			}
		}
	}
	b.pumpLocked()
}

func (b *Broker) managementLocked(c *conn, l *link, m *amqp.Message) error {
	op, _ := m.ApplicationProperties["operation"].(string)
	typ, _ := m.ApplicationProperties["type"].(string)
	name, _ := m.ApplicationProperties["name"].(string)
	if entity := strings.TrimSuffix(l.address, "/$management"); entity != l.address {
		if q := b.queueFor(entity); q != nil && b.serviceBusManagementLocked(c, m, op, q) {
			b.pumpLocked()
			return nil
		}
	}
	if op != "READ" || (typ != "com.microsoft:eventhub" && typ != "com.microsoft:partition") {
		b.replyLocked(c, m, 400, "unsupported operation", nil)
		return nil
	}
	h := b.hubs[name]
	if h == nil {
		b.replyLocked(c, m, 404, "The messaging entity '"+name+"' could not be found.", nil)
		return nil
	}
	if typ == "com.microsoft:partition" {
		b.partitionPropertiesLocked(c, m, h)
		return nil
	}
	ids := make([]string, len(h.partitions))
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}
	b.replyLocked(c, m, 200, "OK", map[string]any{"name": name, "type": typ, "partition_count": int32(len(ids)),
		"partition_ids": ids, "created_at": time.Unix(1700000000, 0), "georeplication_factor": int32(1)})
	return nil
}

func (b *Broker) cbsLocked(c *conn, m *amqp.Message) error {
	op, _ := m.ApplicationProperties["operation"].(string)
	audience, _ := m.ApplicationProperties["name"].(string)
	token, _ := m.Value.(string)
	if op != "put-token" {
		b.replyLocked(c, m, 400, "unsupported operation", nil)
		return nil
	}
	if b.opts.PutToken != nil && !b.opts.PutToken(audience, token) {
		b.replyLocked(c, m, 401, "Unauthorized", nil)
		return nil
	}
	b.tokens = append(b.tokens, token)
	b.replyLocked(c, m, 202, "Accepted", nil)
	return nil
}

// pumpLocked sends what every broker-side sender link has credit for.
func (b *Broker) pumpLocked() {
	for c := range b.conns {
		for _, s := range c.sessions {
			ids := make([]uint32, 0, len(s.links))
			for h := range s.links {
				ids = append(ids, h)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			for _, h := range ids {
				l := s.links[h]
				if !l.sender {
					continue
				}
				for l.credit > 0 {
					var st *stored
					switch {
					case len(l.pending) > 0:
						st, l.pending = l.pending[0], l.pending[1:]
					case l.q != nil:
						st = l.q.takeLocked(l)
					case l.hub != nil && l.pos < len(l.hub.partitions[l.partition]):
						st = &l.hub.partitions[l.partition][l.pos].stored
						l.pos++
					}
					if st == nil {
						break
					}
					id := s.nextDelivery
					s.nextDelivery++
					l.credit--
					l.count++
					if l.q != nil {
						l.unsettled[id] = st
					}
					payload := st.payload
					if l.q != nil {
						payload = deliveryPayload(l.q, st)
					} else if st.count > 0 {
						// The header's delivery-count says it was tried before.
						cp := *st.msg
						hdr := amqp.MessageHeader{DeliveryCount: st.count}
						if cp.Header != nil {
							hdr = *cp.Header
							hdr.DeliveryCount = st.count
						}
						cp.Header = &hdr
						payload, _ = cp.MarshalBinary()
					}
					tag := make([]byte, 4)
					binary.BigEndian.PutUint32(tag, id)
					if l.q != nil {
						tag = lockToken()
					}
					c.frame(0, s.ch, func(w *writer) {
						w.described(codeTransfer, func(w *writer) {
							w.list(func(w *writer) { w.uint(l.ours) }, func(w *writer) { w.uint(id) }, func(w *writer) { w.bin(tag) },
								func(w *writer) { w.uint(0) }, func(w *writer) { w.boolean(l.q == nil && l.hub == nil) })
						})
					}, payload)
				}
			}
		}
	}
}

// releaseLinkLocked puts a closing link's unsettled queue messages back.
func (b *Broker) releaseLinkLocked(l *link) {
	if l.q == nil {
		return
	}
	ids := make([]uint32, 0, len(l.unsettled))
	for id := range l.unsettled {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		b.redeliverLocked(l.q, l.unsettled[id])
	}
	l.unsettled = map[uint32]*stored{}
	if l.session != "" && l.q.locks[l.session] == l {
		delete(l.q.locks, l.session)
	}
}

func (c *conn) releaseLocked() {
	for _, s := range c.sessions {
		for _, l := range s.links {
			c.b.releaseLinkLocked(l)
		}
	}
	c.sessions = map[uint16]*session{}
}

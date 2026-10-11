package amqpsim

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// The AMQP 1.0 type system (OASIS AMQP 1.0, part 1): enough of it to read
// and write the transport performatives, SASL frames, sources, targets,
// delivery states and errors. Message sections are left to go-amqp's own
// Message.MarshalBinary and UnmarshalBinary.

// Symbol is an AMQP symbol.
type Symbol string

// Described is a described value with a numeric or symbolic descriptor.
type Described struct {
	Code  uint64
	Name  Symbol
	Value any
}

// ---- decoding -----------------------------------------------------------

type reader struct {
	b   []byte
	err error
}

func (r *reader) fail(f string, a ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(f, a...)
	}
	r.b = nil
}

func (r *reader) take(n int) []byte {
	if n < 0 || len(r.b) < n {
		r.fail("short AMQP value")
		return make([]byte, max(n, 0))
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() byte    { return r.take(1)[0] }
func (r *reader) u16() uint16 { return binary.BigEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.BigEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.BigEndian.Uint64(r.take(8)) }

// value reads one encoded value.
func (r *reader) value() any {
	if r.err != nil {
		return nil
	}
	c := r.u8()
	return r.typed(c)
}

func (r *reader) typed(c byte) any {
	switch c {
	case 0x00:
		d := r.value()
		v := r.value()
		out := Described{Value: v}
		switch x := d.(type) {
		case uint64:
			out.Code = x
		case Symbol:
			out.Name = x
		}
		return out
	case 0x40:
		return nil
	case 0x41:
		return true
	case 0x42:
		return false
	case 0x56:
		return r.u8() != 0
	// ubyte, ushort and uint keep their width, so a value echoed back
	// (a link's source and target in attach) has the type the client
	// sent: go-amqp refuses a ulong where the field is a uint.
	case 0x50:
		return r.u8()
	case 0x60:
		return r.u16()
	case 0x70:
		return r.u32()
	case 0x52:
		return uint32(r.u8())
	case 0x43:
		return uint32(0)
	case 0x80:
		return r.u64()
	case 0x53:
		return uint64(r.u8())
	case 0x44:
		return uint64(0)
	case 0x51:
		return int64(int8(r.u8()))
	case 0x61:
		return int64(int16(r.u16()))
	case 0x71:
		return int64(int32(r.u32()))
	case 0x54:
		return int64(int8(r.u8()))
	case 0x81:
		return int64(r.u64())
	case 0x55:
		return int64(int8(r.u8()))
	case 0x72:
		return float64(math.Float32frombits(r.u32()))
	case 0x82:
		return math.Float64frombits(r.u64())
	case 0x73:
		return rune(r.u32())
	case 0x83:
		return time.UnixMilli(int64(r.u64())).UTC()
	case 0x98:
		var u [16]byte
		copy(u[:], r.take(16))
		return u
	case 0xa0:
		return append([]byte(nil), r.take(int(r.u8()))...)
	case 0xb0:
		return append([]byte(nil), r.take(int(r.u32()))...)
	case 0xa1:
		return string(r.take(int(r.u8())))
	case 0xb1:
		return string(r.take(int(r.u32())))
	case 0xa3:
		return Symbol(r.take(int(r.u8())))
	case 0xb3:
		return Symbol(r.take(int(r.u32())))
	case 0x45:
		return []any{}
	case 0xc0, 0xd0:
		var size, count int
		if c == 0xc0 {
			size, count = int(r.u8()), 0
			if size > 0 {
				count = int(r.u8())
				size--
			}
		} else {
			size = int(r.u32())
			count = int(r.u32())
			size -= 4
		}
		sub := &reader{b: r.take(size)}
		out := make([]any, 0, count)
		for i := 0; i < count && sub.err == nil; i++ {
			out = append(out, sub.value())
		}
		if sub.err != nil {
			r.fail("%v", sub.err)
		}
		return out
	case 0xc1, 0xd1:
		var size, count int
		if c == 0xc1 {
			size = int(r.u8())
			count = int(r.u8())
			size--
		} else {
			size = int(r.u32())
			count = int(r.u32())
			size -= 4
		}
		sub := &reader{b: r.take(size)}
		out := map[any]any{}
		for i := 0; i+1 < count+1 && i < count && sub.err == nil; i += 2 {
			k := sub.value()
			v := sub.value()
			out[mapKey(k)] = v
		}
		if sub.err != nil {
			r.fail("%v", sub.err)
		}
		return out
	case 0xe0, 0xf0:
		var size, count int
		if c == 0xe0 {
			size = int(r.u8())
			count = int(r.u8())
			size--
		} else {
			size = int(r.u32())
			count = int(r.u32())
			size -= 4
		}
		sub := &reader{b: r.take(size)}
		et := sub.u8()
		var desc any
		if et == 0x00 {
			desc = sub.value()
			et = sub.u8()
		}
		_ = desc
		out := make([]any, 0, count)
		for i := 0; i < count && sub.err == nil; i++ {
			out = append(out, sub.typed(et))
		}
		return out
	}
	r.fail("AMQP type code 0x%02x not read", c)
	return nil
}

func mapKey(k any) any {
	switch x := k.(type) {
	case []byte:
		return string(x)
	case []any, map[any]any:
		return fmt.Sprint(x)
	}
	return k
}

// field returns list element i of a described list, or nil.
func field(l []any, i int) any {
	if i < len(l) {
		return l[i]
	}
	return nil
}

func asUint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case int64:
		return uint64(x), x >= 0
	}
	return 0, false
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case Symbol:
		return string(x)
	}
	return ""
}

func asBool(v any) bool { b, _ := v.(bool); return b }

// ---- encoding -----------------------------------------------------------

// writer writes AMQP values.
type writer struct{ b []byte }

func (w *writer) null() { w.b = append(w.b, 0x40) }
func (w *writer) boolean(v bool) {
	if v {
		w.b = append(w.b, 0x41)
	} else {
		w.b = append(w.b, 0x42)
	}
}
func (w *writer) ubyte(v uint8) { w.b = append(w.b, 0x50, v) }
func (w *writer) ushort(v uint16) {
	w.b = append(w.b, 0x60)
	w.b = binary.BigEndian.AppendUint16(w.b, v)
}
func (w *writer) uint(v uint32) {
	w.b = append(w.b, 0x70)
	w.b = binary.BigEndian.AppendUint32(w.b, v)
}
func (w *writer) ulong(v uint64) {
	w.b = append(w.b, 0x80)
	w.b = binary.BigEndian.AppendUint64(w.b, v)
}
func (w *writer) long(v int64) {
	w.b = append(w.b, 0x81)
	w.b = binary.BigEndian.AppendUint64(w.b, uint64(v))
}
func (w *writer) int(v int32) {
	w.b = append(w.b, 0x71)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(v))
}
func (w *writer) timestamp(t time.Time) {
	w.b = append(w.b, 0x83)
	w.b = binary.BigEndian.AppendUint64(w.b, uint64(t.UnixMilli()))
}
func (w *writer) str(s string) {
	w.b = append(w.b, 0xb1)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(s)))
	w.b = append(w.b, s...)
}
func (w *writer) sym(s string) {
	w.b = append(w.b, 0xb3)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(s)))
	w.b = append(w.b, s...)
}
func (w *writer) bin(b []byte) {
	w.b = append(w.b, 0xb0)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(b)))
	w.b = append(w.b, b...)
}

// list writes a list32 of the values items write.
func (w *writer) list(items ...func(*writer)) {
	var sub writer
	for _, f := range items {
		f(&sub)
	}
	w.b = append(w.b, 0xd0)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(sub.b)+4))
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(items)))
	w.b = append(w.b, sub.b...)
}

// mapOf writes a map32 of key and value writers.
func (w *writer) mapOf(pairs ...func(*writer)) {
	var sub writer
	for _, f := range pairs {
		f(&sub)
	}
	w.b = append(w.b, 0xd1)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(sub.b)+4))
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(pairs)))
	w.b = append(w.b, sub.b...)
}

// symArray writes an array32 of symbols.
func (w *writer) symArray(s ...string) {
	var sub writer
	sub.b = append(sub.b, 0xb3)
	for _, x := range s {
		sub.b = binary.BigEndian.AppendUint32(sub.b, uint32(len(x)))
		sub.b = append(sub.b, x...)
	}
	w.b = append(w.b, 0xf0)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(sub.b)+4))
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(s)))
	w.b = append(w.b, sub.b...)
}

// strArray writes an array32 of strings.
func (w *writer) strArray(s ...string) {
	var sub writer
	sub.b = append(sub.b, 0xb1)
	for _, x := range s {
		sub.b = binary.BigEndian.AppendUint32(sub.b, uint32(len(x)))
		sub.b = append(sub.b, x...)
	}
	w.b = append(w.b, 0xf0)
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(sub.b)+4))
	w.b = binary.BigEndian.AppendUint32(w.b, uint32(len(s)))
	w.b = append(w.b, sub.b...)
}

// described writes descriptor code then the value f writes.
func (w *writer) described(code uint64, f func(*writer)) {
	w.b = append(w.b, 0x00, 0x53)
	if code > 0xff {
		w.b = w.b[:len(w.b)-1]
		w.b = append(w.b, 0x80)
		w.b = binary.BigEndian.AppendUint64(w.b, code)
	} else {
		w.b = append(w.b, byte(code))
	}
	f(w)
}

// any writes a plain Go value read by reader.value (for echoing sources,
// targets and filters back).
func (w *writer) any(v any) {
	switch x := v.(type) {
	case nil:
		w.null()
	case bool:
		w.boolean(x)
	case uint64:
		w.ulong(x)
	case uint32:
		w.uint(x)
	case uint16:
		w.ushort(x)
	case uint8:
		w.ubyte(x)
	case int64:
		w.long(x)
	case string:
		w.str(x)
	case Symbol:
		w.sym(string(x))
	case []byte:
		w.bin(x)
	case time.Time:
		w.timestamp(x)
	case float64:
		w.b = append(w.b, 0x82)
		w.b = binary.BigEndian.AppendUint64(w.b, math.Float64bits(x))
	case Described:
		if x.Name != "" {
			w.b = append(w.b, 0x00)
			w.sym(string(x.Name))
			w.any(x.Value)
		} else {
			w.described(x.Code, func(w *writer) { w.any(x.Value) })
		}
	case []any:
		items := make([]func(*writer), len(x))
		for i, e := range x {
			e := e
			items[i] = func(w *writer) { w.any(e) }
		}
		w.list(items...)
	case map[any]any:
		var pairs []func(*writer)
		for k, e := range x {
			k, e := k, e
			pairs = append(pairs, func(w *writer) { w.any(k) }, func(w *writer) { w.any(e) })
		}
		w.mapOf(pairs...)
	default:
		w.str(fmt.Sprint(x))
	}
}

// Performative and other descriptor codes.
const (
	codeOpen        = 0x10
	codeBegin       = 0x11
	codeAttach      = 0x12
	codeFlow        = 0x13
	codeTransfer    = 0x14
	codeDisposition = 0x15
	codeDetach      = 0x16
	codeEnd         = 0x17
	codeClose       = 0x18
	codeError       = 0x1d
	codeAccepted    = 0x24
	codeRejected    = 0x25
	codeReleased    = 0x26
	codeModified    = 0x27
	codeSource      = 0x28
	codeTarget      = 0x29
	codeSASLMechs   = 0x40
	codeSASLInit    = 0x41
	codeSASLOutcome = 0x44
)

var errBadFrame = errors.New("malformed AMQP frame")

// performative splits a frame body into its described performative and
// the payload after it.
func performative(body []byte) (code uint64, fields []any, payload []byte, err error) {
	r := &reader{b: body}
	v := r.value()
	if r.err != nil {
		return 0, nil, nil, r.err
	}
	d, ok := v.(Described)
	if !ok {
		return 0, nil, nil, errBadFrame
	}
	l, _ := d.Value.([]any)
	return d.Code, l, r.b, nil
}

// errorValue writes an AMQP error (condition, description).
func errorValue(cond, desc string) func(*writer) {
	return func(w *writer) {
		w.described(codeError, func(w *writer) {
			w.list(func(w *writer) { w.sym(cond) }, func(w *writer) { w.str(desc) })
		})
	}
}

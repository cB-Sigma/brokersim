package amqpsim

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// go-amqp against the simulator: a queue's message is redelivered after a
// release and gone after an accept; a sender to a queue gets accepted, a
// rejected one the hook's text; an unknown address is refused; an event
// hub partition is read from a selector filter; $management lists the
// partitions; a wrong password is refused.
func TestSimWithGoAMQP(t *testing.T) {
	b, err := Start(Options{Users: map[string]string{"sigma": "pw"},
		Reject: func(addr string, m *amqp.Message) error {
			if addr == "cmd" && string(m.GetData()) == "bad" {
				return errTest
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.CreateQueue("telemetry")
	b.CreateQueue("cmd")
	b.CreateEventHub("hub1", 2)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	conn, err := amqp.Dial(ctx, "amqp://"+b.Addr(), &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("sigma", "pw")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.Send("telemetry", amqp.NewMessage([]byte(`{"a":1}`)))
	r, err := sess.NewReceiver(ctx, "telemetry", &amqp.ReceiverOptions{Credit: 10})
	if err != nil {
		t.Fatal(err)
	}
	m, err := r.Receive(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ReleaseMessage(ctx, m); err != nil {
		t.Fatal(err)
	}
	m, err = r.Receive(ctx, nil)
	if err != nil || m.Header == nil || m.Header.DeliveryCount != 1 {
		t.Fatalf("redelivery %+v %v", m, err)
	}
	if err := r.AcceptMessage(ctx, m); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if ready, inflight := b.Depth("telemetry"); ready != 0 || inflight != 0 {
		t.Fatalf("depth %d %d", ready, inflight)
	}
	snd, err := sess.NewSender(ctx, "cmd", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := snd.Send(ctx, amqp.NewMessage([]byte("good")), nil); err != nil {
		t.Fatal(err)
	}
	if err := snd.Send(ctx, amqp.NewMessage([]byte("bad")), nil); err == nil || !strings.Contains(err.Error(), "test refusal") {
		t.Fatalf("rejected: %v", err)
	}
	if _, err := sess.NewSender(ctx, "nowhere", nil); err == nil || !strings.Contains(err.Error(), "not-found") {
		t.Fatalf("unknown address: %v", err)
	}
	for i := 0; i < 3; i++ {
		b.SendEvent("hub1", 1, amqp.NewMessage([]byte{byte('0' + i)}), map[string]any{"iothub-connection-device-id": "dev"})
	}
	hr, err := sess.NewReceiver(ctx, "hub1/ConsumerGroups/$Default/Partitions/1", &amqp.ReceiverOptions{Credit: 10,
		Filters: []amqp.LinkFilter{amqp.NewSelectorFilter("amqp.annotation.x-opt-sequence-number > '0'")}})
	if err != nil {
		t.Fatal(err)
	}
	ev, err := hr.Receive(ctx, nil)
	if err != nil || string(ev.GetData()) != "1" || ev.Annotations["iothub-connection-device-id"] != "dev" {
		t.Fatalf("event %+v %v", ev, err)
	}
	// $management READ.
	reply := "mgmt-reply"
	mr, err := sess.NewReceiver(ctx, "$management", &amqp.ReceiverOptions{TargetAddress: reply, Credit: 5})
	if err != nil {
		t.Fatal(err)
	}
	ms, err := sess.NewSender(ctx, "$management", nil)
	if err != nil {
		t.Fatal(err)
	}
	req := &amqp.Message{ApplicationProperties: map[string]any{"operation": "READ", "type": "com.microsoft:eventhub", "name": "hub1"},
		Properties: &amqp.MessageProperties{MessageID: "1", ReplyTo: &reply}}
	if err := ms.Send(ctx, req, nil); err != nil {
		t.Fatal(err)
	}
	resp, err := mr.Receive(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := resp.Value.(map[string]any)
	if ids, _ := v["partition_ids"].([]string); len(ids) != 2 {
		t.Fatalf("management %#v", resp.Value)
	}
	if _, err := amqp.Dial(ctx, "amqp://"+b.Addr(), &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("sigma", "no")}); err == nil {
		t.Fatal("wrong password accepted")
	}
}

var errTest = testErr("test refusal")

type testErr string

func (e testErr) Error() string { return string(e) }

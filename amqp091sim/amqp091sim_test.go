package amqp091sim

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// amqp091-go against the simulator: a topic binding routes a published
// message to a durable queue, a nacked delivery comes back marked
// redelivered, an ack removes it, a mandatory publish with no route is
// returned and still confirmed, a missing exchange closes the channel
// with 404 and a wrong password is refused with 403.
func TestSimWithAMQP091Go(t *testing.T) {
	b, err := Start(Options{Users: map[string]string{"sigma": "pw"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	conn, err := amqp.Dial("amqp://sigma:pw@" + b.Addr() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueDeclare("q1", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind("q1", "plant.*.telemetry", "amq.topic", false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.Qos(10, 0, false); err != nil {
		t.Fatal(err)
	}
	ds, err := ch.Consume("q1", "c1", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "amq.topic", "plant.p1.telemetry", true, false,
		amqp.Publishing{ContentType: "application/json", CorrelationId: "c-1", Headers: amqp.Table{"h": "v"}, Body: []byte(`{"a":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := dc.WaitContext(ctx); !ok || err != nil {
		t.Fatalf("confirm %v %v", ok, err)
	}
	d := <-ds
	if d.RoutingKey != "plant.p1.telemetry" || d.CorrelationId != "c-1" || d.Headers["h"] != "v" || d.Redelivered {
		t.Fatalf("delivery %+v", d)
	}
	d.Nack(false, true)
	d = <-ds
	if !d.Redelivered {
		t.Fatal("not redelivered")
	}
	d.Ack(false)
	time.Sleep(50 * time.Millisecond)
	if r, u := b.Depth("q1"); r != 0 || u != 0 {
		t.Fatalf("depth %d %d", r, u)
	}
	dc, _ = ch.PublishWithDeferredConfirmWithContext(ctx, "amq.topic", "nowhere", true, false, amqp.Publishing{Body: []byte("x")})
	select {
	case r := <-returns:
		if r.ReplyCode != 312 {
			t.Fatalf("return %+v", r)
		}
	case <-ctx.Done():
		t.Fatal("no return")
	}
	if ok, _ := dc.WaitContext(ctx); !ok {
		t.Fatal("unroutable publish not confirmed")
	}
	ch2, _ := conn.Channel()
	closed := ch2.NotifyClose(make(chan *amqp.Error, 1))
	ch2.PublishWithContext(ctx, "no-such-exchange", "k", false, false, amqp.Publishing{Body: []byte("x")})
	select {
	case e := <-closed:
		if e == nil || e.Code != 404 {
			t.Fatalf("close %v", e)
		}
	case <-ctx.Done():
		t.Fatal("channel not closed")
	}
	if _, err := amqp.Dial("amqp://sigma:no@" + b.Addr() + "/"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("wrong password: %v", err)
	}
}

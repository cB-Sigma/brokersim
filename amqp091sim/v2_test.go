// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package amqp091sim_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cB-Sigma/brokersim/amqp091sim"
	"github.com/cB-Sigma/brokersim/testcert"
)

// Over TLS a client publishes and gets a message; a durable queue and its
// messages survive a restart through Snapshot and Restore, a transient
// one does not; basic.get, the headers exchange and a transaction work;
// x-max-length drops the oldest; purge, unbind and the deletes work.
// (The same behaviours are compared with RabbitMQ in the interop module.)
// Covers: SIG-IPE-6781
func TestTLSSnapshotAndMore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cert, err := testcert.New()
	if err != nil {
		t.Fatal(err)
	}
	b, err := amqp091sim.Start(amqp091sim.Options{TLS: cert.ServerConfig()})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := amqp.DialTLS("amqps://guest:guest@"+b.Addr()+"/", cert.ClientConfig())
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := conn.Channel()
	if _, err := ch.QueueDeclare("keep", true, false, false, false, amqp.Table{"x-max-length": int32(5)}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueDeclare("temp", false, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind("keep", "", "amq.headers", false, amqp.Table{"x-match": "all", "kind": "meter"}); err != nil {
		t.Fatal(err)
	}
	_ = ch.Tx()
	for _, h := range []amqp.Table{{"kind": "meter"}, {"kind": "pump"}} {
		_ = ch.PublishWithContext(ctx, "amq.headers", "", false, false, amqp.Publishing{Headers: h, Body: []byte(h["kind"].(string))})
	}
	_ = ch.PublishWithContext(ctx, "", "temp", false, false, amqp.Publishing{Body: []byte("t")})
	if r, _ := b.Depth("keep"); r != 0 {
		t.Errorf("published before commit: %d", r)
	}
	if err := ch.TxCommit(); err != nil {
		t.Fatal(err)
	}
	if r, _ := b.Depth("keep"); r != 1 {
		t.Errorf("keep holds %d after commit", r)
	}
	ch.Close()
	conn.Close()
	snap := b.Snapshot()
	b.Close()

	b2, err := amqp091sim.Start(amqp091sim.Options{Restore: snap})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	conn2, err := amqp.Dial("amqp://guest:guest@" + b2.Addr() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	ch2, _ := conn2.Channel()
	d, ok, err := ch2.Get("keep", true)
	if err != nil || !ok || string(d.Body) != "meter" {
		t.Errorf("after restore: %v %v %q", ok, err, d.Body)
	}
	if _, err := ch2.QueueDeclarePassive("temp", false, false, false, false, nil); err == nil {
		t.Error("the transient queue survived the restart")
	}
	ch2, _ = conn2.Channel()
	// x-max-length 5 drops the oldest; purge, unbind and the deletes.
	for i := 0; i < 7; i++ {
		_ = ch2.PublishWithContext(ctx, "amq.headers", "", false, false, amqp.Publishing{Headers: amqp.Table{"kind": "meter"}, Body: []byte{'0' + byte(i)}})
	}
	time.Sleep(100 * time.Millisecond)
	if d, _, _ := ch2.Get("keep", true); string(d.Body) != "2" {
		t.Errorf("head after overflow %q", d.Body)
	}
	if n, err := ch2.QueuePurge("keep", false); err != nil || n != 4 {
		t.Errorf("purge %d %v", n, err)
	}
	if err := ch2.QueueUnbind("keep", "", "amq.headers", amqp.Table{"x-match": "all", "kind": "meter"}); err != nil {
		t.Fatal(err)
	}
	_ = ch2.PublishWithContext(ctx, "amq.headers", "", false, false, amqp.Publishing{Headers: amqp.Table{"kind": "meter"}, Body: []byte("x")})
	time.Sleep(100 * time.Millisecond)
	if r, _ := b2.Depth("keep"); r != 0 {
		t.Errorf("routed after unbind: %d", r)
	}
	if err := ch2.ExchangeDeclare("x1", "fanout", false, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch2.ExchangeDelete("x1", false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := ch2.QueueDelete("keep", false, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := ch2.QueueDeclarePassive("keep", true, false, false, false, nil); err == nil {
		t.Error("the deleted queue is still there")
	}
	ch3, _ := conn2.Channel()
	if err := ch3.ExchangeDeclarePassive("x1", "fanout", false, false, false, false, nil); err == nil {
		t.Error("the deleted exchange is still there")
	}
}

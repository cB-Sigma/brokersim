// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package interop compares the fakes with the real brokers in Docker. It
// is skipped unless SIGMA_BROKERSIM_INTEROP=1 is set and Docker answers.
package interop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cB-Sigma/brokersim/amqp091sim"
)

// rabbitImage is the real broker: RabbitMQ from Docker Hub's official
// image (MPL-2.0).
const rabbitImage = "rabbitmq:4.1.4-alpine"

func needDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SIGMA_BROKERSIM_INTEROP") != "1" {
		t.Skip("set SIGMA_BROKERSIM_INTEROP=1 to compare with RabbitMQ in Docker")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker is not running")
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startRabbit runs RabbitMQ and waits until it takes logins.
func startRabbit(t *testing.T) string {
	t.Helper()
	port := freePort(t)
	name := fmt.Sprintf("sigma-brokersim-rabbit-%d", time.Now().UnixNano())
	if out, err := exec.Command("docker", "run", "-d", "--rm", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:5672", port), rabbitImage).CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	url := fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", port)
	deadline := time.Now().Add(90 * time.Second)
	for {
		if c, err := amqp.Dial(url); err == nil {
			_ = c.Close()
			return url
		}
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Fatalf("RabbitMQ did not start:\n%s", logs)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// scenario drives a broker through the behaviours the fake claims and
// writes down what a client sees, one line per observation.
func scenario(t *testing.T, url string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var out []string
	say := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Topic routing, confirms, returns.
	must(ch.ExchangeDeclare("ex.topic", "topic", true, false, false, false, nil))
	_, err = ch.QueueDeclare("q1", true, false, false, false, nil)
	must(err)
	must(ch.QueueBind("q1", "a.*", "ex.topic", false, nil))
	must(ch.Confirm(false))
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))
	returns := ch.NotifyReturn(make(chan amqp.Return, 8))
	must(ch.PublishWithContext(ctx, "ex.topic", "a.b", false, false, amqp.Publishing{Body: []byte("one")}))
	c := <-confirms
	say("confirm %d ack=%v", c.DeliveryTag, c.Ack)
	must(ch.PublishWithContext(ctx, "ex.topic", "z.z", true, false, amqp.Publishing{Body: []byte("lost")}))
	r := <-returns
	say("return %d %s %s %s", r.ReplyCode, r.ReplyText, r.RoutingKey, r.Body)
	<-confirms
	// basic.get.
	d, ok, err := ch.Get("q1", false)
	must(err)
	say("get ok=%v body=%s redelivered=%v remaining=%d", ok, d.Body, d.Redelivered, d.MessageCount)
	must(d.Nack(false, true))
	d, ok, _ = ch.Get("q1", true)
	say("get again ok=%v body=%s redelivered=%v", ok, d.Body, d.Redelivered)
	_, ok, _ = ch.Get("q1", true)
	say("get empty ok=%v", ok)

	// Headers exchange.
	ch2, _ := conn.Channel()
	_, err = ch2.QueueDeclare("qh", false, false, false, false, nil)
	must(err)
	must(ch2.QueueBind("qh", "", "amq.headers", false, amqp.Table{"x-match": "any", "kind": "meter", "site": "s1"}))
	must(ch2.QueueBind("qh", "", "amq.headers", false, amqp.Table{"x-match": "all", "kind": "pump", "site": "s2"}))
	for _, h := range []amqp.Table{{"kind": "meter"}, {"site": "s1", "x": 1}, {"kind": "pump"}, {"kind": "pump", "site": "s2"}, {"other": 1}} {
		must(ch2.PublishWithContext(ctx, "amq.headers", "", false, false, amqp.Publishing{Headers: h, Body: []byte(fmt.Sprint(h))}))
	}
	time.Sleep(300 * time.Millisecond)
	q, err := ch2.QueueDeclarePassive("qh", false, false, false, false, nil)
	must(err)
	say("headers routed %d", q.Messages)

	// Transactions.
	ch3, _ := conn.Channel()
	_, err = ch3.QueueDeclare("qt", false, false, false, false, nil)
	must(err)
	must(ch3.Tx())
	must(ch3.PublishWithContext(ctx, "", "qt", false, false, amqp.Publishing{Body: []byte("rolled back")}))
	must(ch3.TxRollback())
	q, _ = ch3.QueueDeclarePassive("qt", false, false, false, false, nil)
	say("after rollback %d", q.Messages)
	must(ch3.PublishWithContext(ctx, "", "qt", false, false, amqp.Publishing{Body: []byte("kept")}))
	must(ch3.TxCommit())
	time.Sleep(200 * time.Millisecond)
	q, _ = ch3.QueueDeclarePassive("qt", false, false, false, false, nil)
	say("after commit %d", q.Messages)

	// Dead-lettering on reject and on TTL.
	ch4, _ := conn.Channel()
	must(ch4.ExchangeDeclare("dlx", "fanout", false, false, false, false, nil))
	_, err = ch4.QueueDeclare("dlq", false, false, false, false, nil)
	must(err)
	must(ch4.QueueBind("dlq", "", "dlx", false, nil))
	_, err = ch4.QueueDeclare("qd", false, false, false, false, amqp.Table{"x-dead-letter-exchange": "dlx", "x-message-ttl": int32(100)})
	must(err)
	must(ch4.PublishWithContext(ctx, "", "qd", false, false, amqp.Publishing{Body: []byte("rejected")}))
	d, ok, _ = ch4.Get("qd", false)
	say("dl get ok=%v %s", ok, d.Body)
	must(d.Reject(false))
	must(ch4.PublishWithContext(ctx, "", "qd", false, false, amqp.Publishing{Body: []byte("expired")}))
	time.Sleep(400 * time.Millisecond)
	_, ok, _ = ch4.Get("qd", true)
	say("qd after ttl ok=%v", ok)
	for i := 0; i < 2; i++ {
		d, ok, _ = ch4.Get("dlq", true)
		reason := ""
		if deaths, _ := d.Headers["x-death"].([]any); len(deaths) > 0 {
			if m, ok := deaths[0].(amqp.Table); ok {
				reason = fmt.Sprint(m["reason"], " ", m["queue"])
			}
		}
		say("dlq ok=%v %s reason=%s first=%v", ok, d.Body, reason, d.Headers["x-first-death-reason"])
	}

	// Purge and delete.
	ch5, _ := conn.Channel()
	_, err = ch5.QueueDeclare("qp", false, false, false, false, nil)
	must(err)
	for i := 0; i < 3; i++ {
		must(ch5.PublishWithContext(ctx, "", "qp", false, false, amqp.Publishing{Body: []byte("x")}))
	}
	time.Sleep(200 * time.Millisecond)
	n, err := ch5.QueuePurge("qp", false)
	say("purged %d %v", n, err)
	must(ch5.PublishWithContext(ctx, "", "qp", false, false, amqp.Publishing{Body: []byte("x")}))
	time.Sleep(200 * time.Millisecond)
	n, err = ch5.QueueDelete("qp", false, false, false)
	say("deleted %d %v", n, err)

	// Channel errors.
	ch6, _ := conn.Channel()
	_, err = ch6.QueueDeclarePassive("nope", false, false, false, false, nil)
	var ae *amqp.Error
	if errors.As(err, &ae) {
		say("passive missing queue %d", ae.Code)
	}
	ch7, _ := conn.Channel()
	err = ch7.ExchangeDeclare("ex.topic", "fanout", true, false, false, false, nil)
	if errors.As(err, &ae) {
		say("inequivalent exchange %d", ae.Code)
	}
	ch8, _ := conn.Channel()
	must(ch8.Qos(1, 0, false))
	_, err = ch8.QueueDeclare("qc", false, false, false, false, nil)
	must(err)
	for _, b := range []string{"c1", "c2"} {
		must(ch8.PublishWithContext(ctx, "", "qc", false, false, amqp.Publishing{Body: []byte(b)}))
	}
	msgs, err := ch8.Consume("qc", "", false, false, false, false, nil)
	must(err)
	m1 := <-msgs
	select {
	case m := <-msgs:
		say("prefetch 1 broken: got %s", m.Body)
	case <-time.After(300 * time.Millisecond):
		say("prefetch holds after %s", m1.Body)
	}
	must(m1.Nack(false, true))
	m := <-msgs
	say("after nack %s redelivered=%v", m.Body, m.Redelivered)
	return out
}

// TestScenarioAgainstRabbitMQ runs the same scenario on the fake and on
// RabbitMQ and requires the same observations.
// Covers: SIG-IPE-6780
func TestScenarioAgainstRabbitMQ(t *testing.T) {
	needDocker(t)
	fake, err := amqp091sim.Start(amqp091sim.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close()
	got := scenario(t, "amqp://guest:guest@"+fake.Addr()+"/")
	want := scenario(t, startRabbit(t))
	t.Logf("RabbitMQ:\n%s", strings.Join(want, "\n"))
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		for i := range max(len(got), len(want)) {
			var g, w string
			if i < len(got) {
				g = got[i]
			}
			if i < len(want) {
				w = want[i]
			}
			if g != w {
				t.Errorf("line %d: fake %q, RabbitMQ %q", i, g, w)
			}
		}
	}
}

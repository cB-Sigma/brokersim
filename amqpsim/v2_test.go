// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package amqpsim_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/cB-Sigma/brokersim/amqpsim"
	"github.com/cB-Sigma/brokersim/testcert"
)

// Over amqps a client sends and receives; a message released past the
// maximum delivery count lands in the dead-letter subqueue; the throttle
// hook refuses with com.microsoft:server-busy and an over-size message
// with amqp:link:message-size-exceeded.
// Covers: SIG-IPE-6782
func TestTLSDeadLetterThrottle(t *testing.T) {
	cert, err := testcert.New()
	if err != nil {
		t.Fatal(err)
	}
	var busy atomic.Bool
	b, err := amqpsim.Start(amqpsim.Options{TLS: cert.ServerConfig(), MaxSize: 2048,
		Throttle: func(string) error {
			if busy.Load() {
				return errors.New("The request was terminated because the entity is being throttled.")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.CreateQueueWith("q", amqpsim.QueueOptions{MaxDeliveryCount: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := amqp.Dial(ctx, "amqps://"+b.Addr(), &amqp.ConnOptions{TLSConfig: cert.ClientConfig()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sess, _ := conn.NewSession(ctx, nil)
	snd, err := sess.NewSender(ctx, "q", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := snd.Send(ctx, amqp.NewMessage([]byte("poison")), nil); err != nil {
		t.Fatal(err)
	}
	rcv, _ := sess.NewReceiver(ctx, "q", nil)
	for i := 0; i < 2; i++ {
		m, err := rcv.Receive(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = rcv.ReleaseMessage(ctx, m)
	}
	time.Sleep(100 * time.Millisecond)
	if dl := b.DeadLetters("q"); len(dl) != 1 || dl[0].ApplicationProperties["DeadLetterReason"] != "MaxDeliveryCountExceeded" {
		t.Errorf("dead letters %v", dl)
	}
	busy.Store(true)
	var ae *amqp.Error
	if err := snd.Send(ctx, amqp.NewMessage([]byte("x")), nil); !errors.As(err, &ae) || ae.Condition != "com.microsoft:server-busy" {
		t.Errorf("throttled send: %v", err)
	}
	busy.Store(false)
	if err := snd.Send(ctx, amqp.NewMessage([]byte(strings.Repeat("x", 4096))), nil); !errors.As(err, &ae) || ae.Condition != "amqp:link:message-size-exceeded" {
		t.Errorf("over-size send: %v", err)
	}
}

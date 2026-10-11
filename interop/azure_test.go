// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package interop

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/cB-Sigma/brokersim/amqpsim"
)

// emulatorConn is a connection string in the Azure SDKs' development
// emulator form (plain AMQP to the given host and port).
func emulatorConn(addr, entity string) string {
	s := "Endpoint=sb://" + addr + ";SharedAccessKeyName=demo;SharedAccessKey=ZGVtby1rZXk=;UseDevelopmentEmulator=true"
	if entity != "" {
		s += ";EntityPath=" + entity
	}
	return s
}

func recv(t *testing.T, r *azservicebus.Receiver, n int) []*azservicebus.ReceivedMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []*azservicebus.ReceivedMessage
	for len(out) < n {
		ms, err := r.ReceiveMessages(ctx, n-len(out), nil)
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		out = append(out, ms...)
	}
	return out
}

// TestAzServiceBusSDK runs the Azure SDK for Go's Service Bus client
// (azservicebus v1.11.0) against the fake: send and receive with peek
// lock, complete, abandon with the delivery count raised, dead-letter to
// the dead-letter subqueue with its reason, scheduled messages and
// message sessions.
// Covers: SIG-IPE-6782
func TestAzServiceBusSDK(t *testing.T) {
	b, err := amqpsim.Start(amqpsim.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.CreateQueue("orders")
	b.CreateQueueWith("sessions", amqpsim.QueueOptions{RequiresSession: true})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := azservicebus.NewClientFromConnectionString(emulatorConn(b.Addr(), ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	s, err := c.NewSender("orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"a", "b", "c"} {
		if err := s.SendMessage(ctx, &azservicebus.Message{Body: []byte(body), ApplicationProperties: map[string]any{"k": body}}, nil); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	r, err := c.NewReceiverForQueue("orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	ms := recv(t, r, 3)
	if string(ms[0].Body) != "a" || ms[0].DeliveryCount != 1 || ms[0].SequenceNumber == nil || ms[0].LockedUntil == nil {
		t.Errorf("first message %q count %d seq %v locked %v", ms[0].Body, ms[0].DeliveryCount, ms[0].SequenceNumber, ms[0].LockedUntil)
	}
	if err := r.CompleteMessage(ctx, ms[0], nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := r.AbandonMessage(ctx, ms[1], nil); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	if err := r.DeadLetterMessage(ctx, ms[2], &azservicebus.DeadLetterOptions{Reason: to.Ptr("bad"), ErrorDescription: to.Ptr("not an order")}); err != nil {
		t.Fatalf("dead-letter: %v", err)
	}
	again := recv(t, r, 1)
	if string(again[0].Body) != "b" || again[0].DeliveryCount != 2 {
		t.Errorf("abandoned message came back as %q count %d", again[0].Body, again[0].DeliveryCount)
	}
	_ = r.CompleteMessage(ctx, again[0], nil)
	dlr, err := c.NewReceiverForQueue("orders", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	if err != nil {
		t.Fatal(err)
	}
	dl := recv(t, dlr, 1)
	if string(dl[0].Body) != "c" || dl[0].DeadLetterReason == nil || *dl[0].DeadLetterReason != "bad" ||
		dl[0].DeadLetterErrorDescription == nil || *dl[0].DeadLetterErrorDescription != "not an order" {
		t.Errorf("dead letter %q reason %v", dl[0].Body, dl[0].DeadLetterReason)
	}
	_ = dlr.CompleteMessage(ctx, dl[0], nil)

	// Scheduled: not delivered before its time.
	at := time.Now().Add(1500 * time.Millisecond)
	if _, err := s.ScheduleMessages(ctx, []*azservicebus.Message{{Body: []byte("later")}}, at, nil); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	early, cancelEarly := context.WithTimeout(ctx, 700*time.Millisecond)
	got, _ := r.ReceiveMessages(early, 1, nil)
	cancelEarly()
	if len(got) != 0 {
		t.Errorf("a scheduled message came early: %q", got[0].Body)
	}
	late := recv(t, r, 1)
	if string(late[0].Body) != "later" || time.Now().Before(at) {
		t.Errorf("scheduled message %q at %v", late[0].Body, time.Now())
	}
	_ = r.CompleteMessage(ctx, late[0], nil)

	// Sessions: a session receiver gets only its session's messages.
	ss, _ := c.NewSender("sessions", nil)
	for _, m := range []struct{ sid, body string }{{"s1", "1a"}, {"s2", "2a"}, {"s1", "1b"}} {
		if err := ss.SendMessage(ctx, &azservicebus.Message{Body: []byte(m.body), SessionID: to.Ptr(m.sid)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	sr, err := c.AcceptSessionForQueue(ctx, "sessions", "s1", nil)
	if err != nil {
		t.Fatalf("accept session: %v", err)
	}
	var bodies []string
	sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
	for len(bodies) < 2 {
		ms, err := sr.ReceiveMessages(sctx, 5, nil)
		if err != nil {
			break
		}
		for _, m := range ms {
			bodies = append(bodies, string(m.Body))
			_ = sr.CompleteMessage(ctx, m, nil)
		}
	}
	scancel()
	sort.Strings(bodies)
	if len(bodies) != 2 || bodies[0] != "1a" || bodies[1] != "1b" {
		t.Errorf("session s1 got %v", bodies)
	}
	next, err := c.AcceptNextSessionForQueue(ctx, "sessions", nil)
	if err != nil || next.SessionID() != "s2" {
		t.Fatalf("next session: %v", err)
	}
	if _, err := c.AcceptSessionForQueue(ctx, "sessions", "s2", nil); err == nil {
		t.Error("a locked session was accepted twice")
	}
	if _, err := c.NewReceiverForQueue("missing", nil); err == nil {
		r2, _ := c.NewReceiverForQueue("missing", nil)
		if _, err := r2.ReceiveMessages(ctx, 1, nil); err == nil {
			t.Error("receiving from a missing queue worked")
		} else {
			var sbe *azservicebus.Error
			if !errors.As(err, &sbe) || sbe.Code != azservicebus.CodeNotFound {
				t.Logf("missing queue: %v", err)
			}
		}
	}
}

// TestAzEventHubsSDK runs the Azure SDK for Go's Event Hubs client
// (azeventhubs v2.0.2) against the fake: hub and partition properties,
// a batch sent to a partition, and a partition client reading it from the
// start with offsets and sequence numbers.
// Covers: SIG-IPE-6782
func TestAzEventHubsSDK(t *testing.T) {
	b, err := amqpsim.Start(amqpsim.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.CreateEventHub("telemetry", 2)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p, err := azeventhubs.NewProducerClientFromConnectionString(emulatorConn(b.Addr(), "telemetry"), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(ctx)
	props, err := p.GetEventHubProperties(ctx, nil)
	if err != nil || len(props.PartitionIDs) != 2 {
		t.Fatalf("hub properties %+v %v", props, err)
	}
	batch, err := p.NewEventDataBatch(ctx, &azeventhubs.EventDataBatchOptions{PartitionID: to.Ptr("1")})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"e1", "e2"} {
		if err := batch.AddEventData(&azeventhubs.EventData{Body: []byte(s)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.SendEventDataBatch(ctx, batch, nil); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	pp, err := p.GetPartitionProperties(ctx, "1", nil)
	if err != nil || pp.LastEnqueuedSequenceNumber != 1 || pp.IsEmpty {
		t.Fatalf("partition properties %+v %v", pp, err)
	}
	c, err := azeventhubs.NewConsumerClientFromConnectionString(emulatorConn(b.Addr(), "telemetry"), "", azeventhubs.DefaultConsumerGroup, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	pc, err := c.NewPartitionClient("1", &azeventhubs.PartitionClientOptions{StartPosition: azeventhubs.StartPosition{Earliest: to.Ptr(true)}})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close(ctx)
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	defer rcancel()
	var evs []*azeventhubs.ReceivedEventData
	for len(evs) < 2 {
		got, err := pc.ReceiveEvents(rctx, 2-len(evs), nil)
		if err != nil {
			t.Fatalf("receive events: %v", err)
		}
		evs = append(evs, got...)
	}
	if string(evs[0].Body) != "e1" || evs[1].SequenceNumber != 1 || evs[0].EnqueuedTime == nil {
		t.Errorf("events %q seq %d", evs[0].Body, evs[1].SequenceNumber)
	}
}

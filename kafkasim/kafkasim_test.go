// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package kafkasim_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/cB-Sigma/brokersim/kafkasim"
	"github.com/cB-Sigma/brokersim/testcert"
)

// A Kafka client logs in as an Event Hubs client does (over TLS, PLAIN
// with $ConnectionString), produces to an event hub and consumes it back;
// a wrong connection string is refused.
// Covers: SIG-IPE-6784
func TestEventHubsKafka(t *testing.T) {
	cert, err := testcert.New()
	if err != nil {
		t.Fatal(err)
	}
	e, err := kafkasim.Start(kafkasim.Options{Hubs: map[string]int32{"telemetry": 2}, TLS: cert.ServerConfig()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := func(pass string) *kgo.Client {
		c, err := kgo.NewClient(kgo.SeedBrokers(e.Addr()), kgo.DialTLSConfig(cert.ClientConfig()),
			kgo.SASL(plain.Auth{User: "$ConnectionString", Pass: pass}.AsMechanism()),
			kgo.ConsumeTopics("telemetry"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := client(e.ConnectionString())
	defer c.Close()
	if err := c.ProduceSync(ctx, &kgo.Record{Topic: "telemetry", Value: []byte("21.5")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	fs := c.PollFetches(ctx)
	if errs := fs.Errors(); len(errs) > 0 {
		t.Fatalf("fetch: %v", errs)
	}
	if recs := fs.Records(); len(recs) != 1 || string(recs[0].Value) != "21.5" {
		t.Errorf("records %v", recs)
	}
	bad := client("Endpoint=sb://wrong/;SharedAccessKeyName=x;SharedAccessKey=y")
	defer bad.Close()
	bctx, bcancel := context.WithTimeout(ctx, 3*time.Second)
	defer bcancel()
	if err := bad.ProduceSync(bctx, &kgo.Record{Topic: "telemetry", Value: []byte("x")}).FirstErr(); err == nil {
		t.Error("a wrong connection string could produce")
	}
}

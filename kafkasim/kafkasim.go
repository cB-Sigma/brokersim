// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package kafkasim is the Kafka endpoint of an Event Hubs namespace, for
// tests: a Kafka cluster (franz-go's kfake) that takes the same SASL login
// Event Hubs does, PLAIN with the user name "$ConnectionString" and the
// namespace's connection string as the password, optionally over TLS as
// Event Hubs requires on port 9093, with each event hub a topic.
package kafkasim

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kfake"
)

// Options configure the endpoint.
type Options struct {
	// Namespace is the namespace host in the connection string (default
	// "demo.servicebus.windows.net").
	Namespace string
	// KeyName and Key are the shared access key in the connection string
	// (default "RootManageSharedAccessKey" and "ZGVtby1rZXk=", demo values).
	KeyName, Key string
	// Hubs are the event hubs (topics) with their partition counts.
	Hubs map[string]int32
	// TLS serves Kafka over TLS (testcert makes a configuration).
	TLS *tls.Config
}

// Endpoint is the running Kafka endpoint.
type Endpoint struct {
	c    *kfake.Cluster
	conn string
}

// Start runs the endpoint on 127.0.0.1.
func Start(o Options) (*Endpoint, error) {
	if o.Namespace == "" {
		o.Namespace = "demo.servicebus.windows.net"
	}
	if o.KeyName == "" {
		o.KeyName = "RootManageSharedAccessKey"
	}
	if o.Key == "" {
		o.Key = "ZGVtby1rZXk="
	}
	conn := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s", o.Namespace, o.KeyName, o.Key)
	opts := []kfake.Opt{kfake.NumBrokers(1), kfake.EnableSASL(), kfake.Superuser("PLAIN", "$ConnectionString", conn)}
	for hub, n := range o.Hubs {
		opts = append(opts, kfake.SeedTopics(n, hub))
	}
	if o.TLS != nil {
		opts = append(opts, kfake.TLS(o.TLS))
	}
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		return nil, err
	}
	return &Endpoint{c: c, conn: conn}, nil
}

// Addr is the bootstrap address (host:port).
func (e *Endpoint) Addr() string { return strings.Join(e.c.ListenAddrs(), ",") }

// ConnectionString is the namespace connection string, the SASL password.
func (e *Endpoint) ConnectionString() string { return e.conn }

// Cluster is the underlying kfake cluster, for its test hooks.
func (e *Endpoint) Cluster() *kfake.Cluster { return e.c }

// Close stops the endpoint.
func (e *Endpoint) Close() { e.c.Close() }

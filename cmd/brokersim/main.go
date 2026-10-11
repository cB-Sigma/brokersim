// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Command brokersim runs the in-process broker fakes on fixed addresses,
// for trying a client by hand: an AMQP 1.0 broker, an AMQP 0-9-1 broker
// with its management API, and an Entra ID token endpoint.
//
//	brokersim -amqp 127.0.0.1:5672 -amqp091 127.0.0.1:5673 -mgmt 127.0.0.1:15672
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/cB-Sigma/brokersim/amqp091sim"
	"github.com/cB-Sigma/brokersim/amqpsim"
	"github.com/cB-Sigma/brokersim/entrasim"
)

func main() {
	stop, err := run(os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "brokersim:", err)
		os.Exit(2)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	stop()
}

// run starts what the flags ask for and returns a function that stops it.
func run(args []string, out io.Writer) (func(), error) {
	fs := flag.NewFlagSet("brokersim", flag.ContinueOnError)
	fs.SetOutput(out)
	a10 := fs.String("amqp", "", "AMQP 1.0 broker address (host:port)")
	queues := fs.String("queues", "demo", "AMQP 1.0 queues to create, comma-separated")
	hub := fs.String("hub", "", "AMQP 1.0 event hub to create (name:partitions)")
	a091 := fs.String("amqp091", "", "AMQP 0-9-1 broker address (host:port)")
	mgmt := fs.String("mgmt", "", "AMQP 0-9-1 management API address (host:port)")
	entra := fs.Bool("entra", false, "start an Entra ID token endpoint (tenant demo-tenant, client demo-app, secret demo-secret)")
	user := fs.String("user", "demo", "user name for SASL PLAIN / AMQPLAIN")
	pass := fs.String("pass", "demo-password", "password")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *a10 == "" && *a091 == "" && !*entra {
		return nil, fmt.Errorf("nothing to run: give -amqp, -amqp091 or -entra")
	}
	var stops []func()
	stop := func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
	users := map[string]string{*user: *pass}
	if *a10 != "" {
		b, err := amqpsim.StartOn(*a10, amqpsim.Options{Users: users, Anonymous: true})
		if err != nil {
			stop()
			return nil, err
		}
		stops = append(stops, b.Close)
		for _, q := range strings.Split(*queues, ",") {
			if q = strings.TrimSpace(q); q != "" {
				b.CreateQueue(q)
			}
		}
		if *hub != "" {
			name, n := *hub, 2
			if i := strings.LastIndex(*hub, ":"); i > 0 {
				name = (*hub)[:i]
				if _, err := fmt.Sscanf((*hub)[i+1:], "%d", &n); err != nil || n < 1 {
					stop()
					return nil, fmt.Errorf("-hub %q: partitions must be a positive number", *hub)
				}
			}
			b.CreateEventHub(name, n)
		}
		fmt.Fprintf(out, "AMQP 1.0 broker on amqp://%s (user %s)\n", b.Addr(), *user)
	}
	if *a091 != "" {
		b, err := amqp091sim.StartOn(*a091, amqp091sim.Options{Users: users})
		if err != nil {
			stop()
			return nil, err
		}
		stops = append(stops, b.Close)
		fmt.Fprintf(out, "AMQP 0-9-1 broker on amqp://%s:%s@%s/\n", *user, *pass, b.Addr())
		if *mgmt != "" {
			ln, err := net.Listen("tcp", *mgmt)
			if err != nil {
				stop()
				return nil, err
			}
			srv := &http.Server{Handler: b.ManagementHandler()}
			go func() { _ = srv.Serve(ln) }()
			stops = append(stops, func() { _ = srv.Close() })
			fmt.Fprintf(out, "AMQP 0-9-1 management API on http://%s/api/\n", ln.Addr())
		}
	}
	if *entra {
		s := entrasim.Start("demo-tenant", "demo-app", "demo-secret", "https://demo.servicebus.windows.net/.default")
		stops = append(stops, s.Close)
		fmt.Fprintf(out, "Entra ID token endpoint on %s/demo-tenant/oauth2/v2.0/token\n", s.URL)
	}
	return stop, nil
}

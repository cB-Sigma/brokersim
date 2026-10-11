// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

func TestRunStartsEveryFake(t *testing.T) {
	var out strings.Builder
	stop, err := run([]string{"-amqp", "127.0.0.1:0", "-hub", "telemetry:4", "-amqp091", "127.0.0.1:0", "-mgmt", "127.0.0.1:0", "-entra"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	for _, want := range []string{"AMQP 1.0 broker on amqp://127.0.0.1:", "AMQP 0-9-1 broker on amqp://demo:", "management API on http://127.0.0.1:", "oauth2/v2.0/token"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}

func TestRunRefusesNothingAndBadHub(t *testing.T) {
	var out strings.Builder
	if _, err := run(nil, &out); err == nil {
		t.Error("no flags: want an error")
	}
	if _, err := run([]string{"-amqp", "127.0.0.1:0", "-hub", "h:x"}, &out); err == nil {
		t.Error("bad -hub: want an error")
	}
}

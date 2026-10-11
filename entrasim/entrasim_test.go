// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package entrasim_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/cB-Sigma/brokersim/entrasim"
)

func token(t *testing.T, s *entrasim.Server, tenant, secret, scope string) (int, map[string]any) {
	t.Helper()
	resp, err := http.PostForm(s.URL+"/"+tenant+"/oauth2/v2.0/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {"demo-app"}, "client_secret": {secret}, "scope": {scope},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestTokenIssuedAndRefused(t *testing.T) {
	s := entrasim.Start("demo-tenant", "demo-app", "demo-secret", "https://example.servicebus.windows.net/.default")
	defer s.Close()

	code, out := token(t, s, "demo-tenant", "demo-secret", "https://example.servicebus.windows.net/.default")
	if code != http.StatusOK || out["token_type"] != "Bearer" {
		t.Fatalf("token: %d %v", code, out)
	}
	if !s.Valid(out["access_token"].(string), "https://example.servicebus.windows.net/.default") {
		t.Error("issued token not valid for its scope")
	}
	if code, out := token(t, s, "demo-tenant", "wrong", "https://example.servicebus.windows.net/.default"); code != http.StatusUnauthorized || out["error"] != "invalid_client" {
		t.Errorf("wrong secret: %d %v", code, out)
	}
	if code, out := token(t, s, "demo-tenant", "demo-secret", "https://other.example/.default"); code != http.StatusBadRequest || out["error"] != "invalid_scope" {
		t.Errorf("unknown scope: %d %v", code, out)
	}
	if code, _ := token(t, s, "other-tenant", "demo-secret", "https://example.servicebus.windows.net/.default"); code != http.StatusBadRequest {
		t.Errorf("unknown tenant: %d", code)
	}
	if s.Calls() != 4 {
		t.Errorf("calls = %d, want 4", s.Calls())
	}
}

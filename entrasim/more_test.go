// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package entrasim_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/cB-Sigma/brokersim/entrasim"
)

// The IMDS endpoint gives a token for a known resource only with the
// Metadata header; the App Service endpoint only with the identity
// header; the tenant's OpenID metadata names the token endpoint.
// Covers: SIG-IPE-6783
func TestManagedIdentityAndMetadata(t *testing.T) {
	s := entrasim.Start("demo-tenant", "demo-app", "demo-secret", "https://vault.example/.default")
	defer s.Close()
	s.IdentityHeader = "h1"
	get := func(path string, hdr map[string]string) (int, map[string]any) {
		req, _ := http.NewRequest(http.MethodGet, s.URL+path, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	q := "?api-version=2018-02-01&resource=https://vault.example"
	if code, out := get(entrasim.IMDSPath+q, map[string]string{"Metadata": "true"}); code != 200 || !s.Valid(out["access_token"].(string), "https://vault.example") || out["expires_on"] == nil {
		t.Errorf("IMDS %d %v", code, out)
	}
	if code, _ := get(entrasim.IMDSPath+q, nil); code != 400 {
		t.Errorf("IMDS without Metadata: %d", code)
	}
	if code, _ := get(entrasim.IMDSPath+"?api-version=2018-02-01&resource=https://other.example", map[string]string{"Metadata": "true"}); code != 400 {
		t.Errorf("IMDS unknown resource: %d", code)
	}
	if code, _ := get(entrasim.AppServicePath+q, map[string]string{"X-IDENTITY-HEADER": "h1"}); code != 200 {
		t.Errorf("App Service: %d", code)
	}
	if code, _ := get(entrasim.AppServicePath+q, map[string]string{"X-IDENTITY-HEADER": "nope"}); code != 401 {
		t.Errorf("App Service wrong header: %d", code)
	}
	if code, out := get("/demo-tenant/v2.0/.well-known/openid-configuration", nil); code != 200 || out["token_endpoint"] != s.URL+"/demo-tenant/oauth2/v2.0/token" {
		t.Errorf("metadata %d %v", code, out)
	}
}

// Package entrasim is an in-process stand-in for the Microsoft identity
// platform's token endpoint, for the OAuth 2.0 client credentials grant
// (https://learn.microsoft.com/entra/identity-platform/v2-oauth2-client-creds-grant-flow):
// POST /{tenant}/oauth2/v2.0/token with grant_type=client_credentials,
// client_id, client_secret and scope (a resource's /.default) as a form;
// the answer is {"token_type":"Bearer","expires_in":…,"access_token":…},
// or 400/401 with {"error":"invalid_client"|"invalid_scope"|…,
// "error_description":…} as the platform words them.
package entrasim

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// Server is the stand-in.
type Server struct {
	*httptest.Server
	Tenant, ClientID, Secret string
	// Scopes are the scopes a token may be issued for.
	Scopes []string
	// ExpiresIn is the tokens' lifetime in seconds (default 3600).
	ExpiresIn int

	mu     sync.Mutex
	issued map[string]string // token to scope
	calls  int
}

// Start starts the stand-in for one tenant and one application.
func Start(tenant, clientID, secret string, scopes ...string) *Server {
	s := &Server{Tenant: tenant, ClientID: clientID, Secret: secret, Scopes: scopes, issued: map[string]string{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// Valid says whether token was issued for scope.
func (s *Server) Valid(token, scope string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.issued[token] == scope
}

// Calls is how many token requests were made.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func fail(w http.ResponseWriter, code int, e, desc string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": e, "error_description": desc})
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	if r.Method != http.MethodPost || r.URL.Path != "/"+s.Tenant+"/oauth2/v2.0/token" {
		fail(w, http.StatusBadRequest, "invalid_request", "AADSTS90002: Tenant not found.")
		return
	}
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
		fail(w, http.StatusBadRequest, "unsupported_grant_type", "AADSTS70003: The grant type is not supported.")
		return
	}
	if r.PostForm.Get("client_id") != s.ClientID || r.PostForm.Get("client_secret") != s.Secret {
		fail(w, http.StatusUnauthorized, "invalid_client", "AADSTS7000215: Invalid client secret provided.")
		return
	}
	scope := r.PostForm.Get("scope")
	ok := false
	for _, sc := range s.Scopes {
		ok = ok || sc == scope
	}
	if !ok || !strings.HasSuffix(scope, "/.default") {
		fail(w, http.StatusBadRequest, "invalid_scope", "AADSTS1002012: The provided value for scope is not valid.")
		return
	}
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	tok := "eyJ0eXAiOiJKV1QifQ." + hex.EncodeToString(b)
	s.mu.Lock()
	s.issued[tok] = scope
	s.mu.Unlock()
	exp := s.ExpiresIn
	if exp == 0 {
		exp = 3600
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"token_type": "Bearer", "expires_in": exp, "ext_expires_in": exp, "access_token": tok})
}

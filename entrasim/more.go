// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package entrasim

// Added in v0.2.0, from the Microsoft identity platform's public
// documentation: certificate credentials (a client assertion, a JWT the
// application signs with its certificate's key, in place of the secret),
// the tenant's OpenID Connect metadata (which MSAL-based clients read
// before asking for a token), the managed identity endpoints of Azure
// virtual machines (IMDS) and of App Service and Functions
// (IDENTITY_ENDPOINT and IDENTITY_HEADER), and TLS.

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"
)

// StartTLS is Start serving HTTPS (httptest's TLS server; s.Client()
// trusts it), as the Azure identity clients require for an authority.
func StartTLS(tenant, clientID, secret string, scopes ...string) *Server {
	s := &Server{Tenant: tenant, ClientID: clientID, Secret: secret, Scopes: scopes, issued: map[string]string{}}
	s.Server = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	return s
}

// Managed identity: requests to /metadata/identity/oauth2/token (IMDS,
// with the "Metadata: true" header) and to /msi/token (App Service, with
// the X-IDENTITY-HEADER header equal to IdentityHeader) get a token for
// the resource asked for when it is among Scopes (as a resource or its
// /.default scope).
const (
	IMDSPath       = "/metadata/identity/oauth2/token"
	AppServicePath = "/msi/token"
)

func (s *Server) scopeOK(scope string) bool {
	for _, sc := range s.Scopes {
		if sc == scope || sc == strings.TrimSuffix(scope, "/")+"/.default" || strings.TrimSuffix(sc, "/.default") == strings.TrimSuffix(scope, "/") {
			return true
		}
	}
	return false
}

func (s *Server) issue(scope string) (string, int) {
	tok := randomToken()
	s.mu.Lock()
	s.issued[tok] = scope
	s.mu.Unlock()
	exp := s.ExpiresIn
	if exp == 0 {
		exp = 3600
	}
	return tok, exp
}

// managedIdentity answers the two managed identity endpoints.
func (s *Server) managedIdentity(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case IMDSPath:
		if r.Header.Get("Metadata") != "true" {
			fail(w, http.StatusBadRequest, "invalid_request", "Required metadata header not specified")
			return true
		}
	case AppServicePath:
		if s.IdentityHeader == "" || r.Header.Get("X-IDENTITY-HEADER") != s.IdentityHeader {
			fail(w, http.StatusUnauthorized, "invalid_request", "The identity header is missing or wrong.")
			return true
		}
	default:
		return false
	}
	resource := r.URL.Query().Get("resource")
	if r.URL.Query().Get("api-version") == "" {
		fail(w, http.StatusBadRequest, "invalid_request", "Required query variable 'api-version' is missing")
		return true
	}
	if !s.scopeOK(resource) {
		fail(w, http.StatusBadRequest, "invalid_resource", "AADSTS500011: The resource principal named "+resource+" was not found in the tenant.")
		return true
	}
	tok, exp := s.issue(resource)
	on := time.Now().Add(time.Duration(exp) * time.Second).Unix()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": tok, "expires_in": strconv.Itoa(exp), "expires_on": strconv.FormatInt(on, 10),
		"resource": resource, "token_type": "Bearer", "client_id": s.ClientID})
	return true
}

// openIDConfig answers the tenant's OpenID Connect metadata.
func (s *Server) openIDConfig(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet || r.URL.Path != "/"+s.Tenant+"/v2.0/.well-known/openid-configuration" {
		return false
	}
	base := s.URL + "/" + s.Tenant
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token_endpoint": base + "/oauth2/v2.0/token", "authorization_endpoint": base + "/oauth2/v2.0/authorize",
		"issuer": base + "/v2.0", "jwks_uri": base + "/discovery/v2.0/keys", "tenant_region_scope": "WW",
		"token_endpoint_auth_methods_supported": []string{"client_secret_post", "private_key_jwt", "client_secret_basic"},
	})
	return true
}

// checkAssertion verifies a client assertion: a JWT signed (RS256, PS256
// or ES256) by the key of one of Certificates, found by the x5t or
// x5t#S256 header, for this client (iss and sub), this token endpoint
// (aud), and valid now.
func (s *Server) checkAssertion(assertion, tokenURL string) error {
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return errors.New("not a JWT")
	}
	var hdr struct {
		Alg     string `json:"alg"`
		X5t     string `json:"x5t"`
		X5tS256 string `json:"x5t#S256"`
	}
	var claims struct {
		Aud any    `json:"aud"`
		Iss string `json:"iss"`
		Sub string `json:"sub"`
		Exp int64  `json:"exp"`
		Nbf int64  `json:"nbf"`
	}
	if err := decodeSegment(parts[0], &hdr); err != nil {
		return err
	}
	if err := decodeSegment(parts[1], &claims); err != nil {
		return err
	}
	var cert *x509.Certificate
	for _, c := range s.Certificates {
		s1 := sha1.Sum(c.Raw)
		s2 := sha256.Sum256(c.Raw)
		// base64url as RFC 7515 has it; standard base64 as MSAL sends it.
		for _, e := range []*base64.Encoding{base64.RawURLEncoding, base64.URLEncoding, base64.StdEncoding, base64.RawStdEncoding} {
			if (hdr.X5t != "" && hdr.X5t == e.EncodeToString(s1[:])) || (hdr.X5tS256 != "" && hdr.X5tS256 == e.EncodeToString(s2[:])) {
				cert = c
			}
		}
	}
	if cert == nil {
		return errors.New("AADSTS700027: The certificate with identifier used to sign the client assertion is not registered on application.")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch hdr.Alg {
	case "RS256":
		pk, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok || rsa.VerifyPKCS1v15(pk, crypto.SHA256, digest[:], sig) != nil {
			return errors.New("AADSTS7000274: The client assertion's signature does not verify.")
		}
	case "PS256":
		pk, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok || rsa.VerifyPSS(pk, crypto.SHA256, digest[:], sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto}) != nil {
			return errors.New("AADSTS7000274: The client assertion's signature does not verify.")
		}
	case "ES256":
		pk, ok := cert.PublicKey.(*ecdsa.PublicKey)
		if !ok || len(sig) != 64 || !ecdsa.Verify(pk, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
			return errors.New("AADSTS7000274: The client assertion's signature does not verify.")
		}
	default:
		return fmt.Errorf("AADSTS5002730: algorithm %q is not supported", hdr.Alg)
	}
	if claims.Iss != s.ClientID || claims.Sub != s.ClientID {
		return errors.New("AADSTS700021: Client assertion application identifier doesn't match 'client_id' parameter.")
	}
	audOK := false
	switch a := claims.Aud.(type) {
	case string:
		audOK = a == tokenURL || strings.HasPrefix(tokenURL, strings.TrimSuffix(a, "/"))
	case []any:
		for _, x := range a {
			audOK = audOK || fmt.Sprint(x) == tokenURL
		}
	}
	if !audOK {
		return errors.New("AADSTS700016: The audience claim of the client assertion is not this token endpoint.")
	}
	now := time.Now().Unix()
	if claims.Exp < now || (claims.Nbf != 0 && claims.Nbf > now+300) {
		return errors.New("AADSTS700024: Client assertion is not within its valid time range.")
	}
	return nil
}

func decodeSegment(seg string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(seg, "="))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

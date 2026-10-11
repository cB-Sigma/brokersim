// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package interop

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/cB-Sigma/brokersim/entrasim"
)

// TestAzIdentity runs the Azure SDK for Go's identity clients (azidentity)
// against the stand-in: a certificate credential (client assertion signed
// with the application's key, after reading the tenant's OpenID metadata),
// a certificate the application did not register refused, and the App
// Service managed identity endpoint.
// Covers: SIG-IPE-6783
func TestAzIdentity(t *testing.T) {
	const scope = "https://demo.servicebus.windows.net/.default"
	s := entrasim.StartTLS("demo-tenant", "demo-app", "", scope)
	defer s.Close()
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "demo-app"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(der)
	s.Certificates = []*x509.Certificate{cert}
	opts := azcore.ClientOptions{Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: s.URL + "/"},
		Transport: s.Client(), Retry: policy.RetryOptions{MaxRetries: -1}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cc, err := azidentity.NewClientCertificateCredential("demo-tenant", "demo-app", []*x509.Certificate{cert}, key,
		&azidentity.ClientCertificateCredentialOptions{ClientOptions: opts, DisableInstanceDiscovery: true})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := cc.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{scope}})
	if err != nil || !s.Valid(tok.Token, scope) {
		t.Fatalf("certificate credential: %v", err)
	}
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	der2, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &other.PublicKey, other)
	cert2, _ := x509.ParseCertificate(der2)
	bad, _ := azidentity.NewClientCertificateCredential("demo-tenant", "demo-app", []*x509.Certificate{cert2}, other,
		&azidentity.ClientCertificateCredentialOptions{ClientOptions: opts, DisableInstanceDiscovery: true})
	if _, err := bad.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{scope}}); err == nil {
		t.Error("an unregistered certificate got a token")
	}

	// App Service managed identity (plain HTTP on the host, as there).
	mi := entrasim.Start("demo-tenant", "demo-app", "", scope)
	defer mi.Close()
	mi.IdentityHeader = "demo-identity-header"
	t.Setenv("IDENTITY_ENDPOINT", mi.URL+entrasim.AppServicePath)
	t.Setenv("IDENTITY_HEADER", "demo-identity-header")
	mc, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
		ClientOptions: azcore.ClientOptions{Transport: http.DefaultClient, Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	mt, err := mc.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{scope}})
	if err != nil {
		t.Fatalf("managed identity: %v", err)
	}
	if !mi.Valid(mt.Token, "https://demo.servicebus.windows.net") && !mi.Valid(mt.Token, scope) {
		t.Errorf("managed identity token not issued by the stand-in")
	}
}

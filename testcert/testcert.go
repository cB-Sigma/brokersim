// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package testcert makes a throwaway certificate authority and a server
// certificate at run time, so the fakes can serve TLS (amqps, AMQP 0-9-1
// over TLS, HTTPS) and the clients under test can trust them. Nothing is
// written to disk; the keys exist only in memory.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// Cert is a CA and a server certificate it issued.
type Cert struct {
	// Pool holds the CA, for a client's RootCAs.
	Pool *x509.CertPool
	// CAPEM is the CA certificate in PEM, for clients configured by file.
	CAPEM []byte
	// Server is the server's certificate and key.
	Server tls.Certificate
}

// New issues a certificate for hosts (DNS names or IP addresses; default
// "localhost" and 127.0.0.1), valid for a day.
func New(hosts ...string) (*Cert, error) {
	if len(hosts) == 0 {
		hosts = []string{"localhost", "127.0.0.1", "::1"}
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "brokersim test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, _ := x509.ParseCertificate(caDER)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: hosts[0]},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			leaf.IPAddresses = append(leaf.IPAddresses, ip)
		} else {
			leaf.DNSNames = append(leaf.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	c := &Cert{Pool: x509.NewCertPool(), CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}
	c.Pool.AddCert(caCert)
	c.Server = tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	return c, nil
}

// ServerConfig is a TLS configuration serving the certificate.
func (c *Cert) ServerConfig() *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{c.Server}, MinVersion: tls.VersionTLS12}
}

// ClientConfig is a TLS configuration trusting the CA.
func (c *Cert) ClientConfig() *tls.Config {
	return &tls.Config{RootCAs: c.Pool, MinVersion: tls.VersionTLS12}
}

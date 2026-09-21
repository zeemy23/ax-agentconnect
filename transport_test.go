// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestParseEndpointStrict(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		secure bool
		want   string
	}{
		{name: "plain host", input: "127.0.0.1:8080", want: "127.0.0.1:8080"},
		{name: "http URL", input: "http://localhost:8080", want: "localhost:8080"},
		{name: "https URL", input: "https://api.example:443", secure: true, want: "api.example:443"},
		{name: "IPv6", input: "[::1]:443", want: "[::1]:443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEndpoint(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got.address != tt.want || got.tls != tt.secure {
				t.Fatalf("parseEndpoint(%q) = %#v, want address %q tls=%v", tt.input, got, tt.want, tt.secure)
			}
		})
	}

	for _, input := range []string{
		"",
		"localhost",
		"localhost:0",
		"localhost:65536",
		"localhost:not-a-port",
		"dns:///service:443",
		"passthrough:///service:443",
		"unix:///tmp/socket",
		"https://host:443/",
		"https://host:443?query=1",
		"https://user:pass@host:443",
		"https://host:443/path",
		"https://host",
		"host:443/extra",
		"host:443#fragment",
		"host:443 with-space",
	} {
		if _, err := parseEndpoint(input); err == nil {
			t.Errorf("parseEndpoint(%q) accepted malformed endpoint", input)
		}
	}
}

func TestNewConnectionPolicyAndTarget(t *testing.T) {
	conn, err := newConnection("127.0.0.1:1", transportConfig{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := conn.Target(), "passthrough:///127.0.0.1:1"; got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
	_ = conn.Close()

	if _, err := newConnection("router.example:443", transportConfig{}, false); err == nil {
		t.Fatal("non-loopback plaintext endpoint accepted without opt-in")
	}
	if conn, err := newConnection("router.example:443", transportConfig{}, true); err != nil {
		t.Fatalf("explicit plaintext opt-in rejected: %v", err)
	} else {
		_ = conn.Close()
	}
	if _, err := newConnection("127.0.0.1:1", transportConfig{CAFile: "unused"}, false); err == nil || !strings.Contains(err.Error(), "https endpoint") {
		t.Fatalf("TLS options on plaintext endpoint error = %v", err)
	}
}

func TestTLSConnectionTrustAndServerName(t *testing.T) {
	ca := newTestCA(t)
	serverCert, serverKey := ca.issue(t, []string{"api.internal"}, []net.IP{net.ParseIP("127.0.0.1")}, false)
	address := startTLSServer(t, serverCert, serverKey, ca, false)

	trusted, err := newConnection("https://"+address, transportConfig{
		CAFile:     ca.certFile,
		ServerName: "api.internal",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer trusted.Close()
	if err := invokeHandshake(trusted); status.Code(err) != codes.Unimplemented {
		t.Fatalf("trusted handshake error = %v, want unknown-method Unimplemented", err)
	}

	untrusted, err := newConnection("https://"+address, transportConfig{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if err := invokeHandshake(untrusted); err == nil || status.Code(err) == codes.Unimplemented {
		t.Fatalf("untrusted handshake error = %v, want TLS verification failure", err)
	}

	wrongName, err := newConnection("https://"+address, transportConfig{
		CAFile:     ca.certFile,
		ServerName: "wrong.internal",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer wrongName.Close()
	if err := invokeHandshake(wrongName); err == nil || status.Code(err) == codes.Unimplemented {
		t.Fatalf("wrong server name handshake error = %v, want TLS verification failure", err)
	}
}

func TestTLSClientCertificate(t *testing.T) {
	ca := newTestCA(t)
	serverCert, serverKey := ca.issue(t, []string{"api.internal"}, []net.IP{net.ParseIP("127.0.0.1")}, false)
	address := startTLSServer(t, serverCert, serverKey, ca, true)

	withoutClient, err := newConnection("https://"+address, transportConfig{
		CAFile:     ca.certFile,
		ServerName: "api.internal",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer withoutClient.Close()
	if err := invokeHandshake(withoutClient); err == nil || status.Code(err) == codes.Unimplemented {
		t.Fatalf("mTLS connection without client certificate error = %v", err)
	}

	clientCert, clientKey := ca.issue(t, nil, nil, true)
	withClient, err := newConnection("https://"+address, transportConfig{
		CAFile:     ca.certFile,
		CertFile:   clientCert,
		KeyFile:    clientKey,
		ServerName: "api.internal",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer withClient.Close()
	if err := invokeHandshake(withClient); status.Code(err) != codes.Unimplemented {
		t.Fatalf("mTLS trusted handshake error = %v, want unknown-method Unimplemented", err)
	}
}

func TestTransportActorMetadataOverride(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"ate-target-actor", "untrusted/actor",
		"x-test", "preserved",
	))
	md, ok := metadata.FromOutgoingContext(actorContext(ctx, "default/fixed"))
	if !ok {
		t.Fatal("actorContext removed outgoing metadata")
	}
	if got := md.Get("ate-target-actor"); len(got) != 1 || got[0] != "default/fixed" {
		t.Fatalf("actor metadata = %v, want one fixed value", got)
	}
	if got := md.Get("x-test"); len(got) != 1 || got[0] != "preserved" {
		t.Fatalf("unrelated metadata = %v, want preserved", got)
	}
}

func invokeHandshake(conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Invoke(ctx, "/transport.Test/Probe", &emptypb.Empty{}, &emptypb.Empty{})
}

type testCA struct {
	cert     *x509.Certificate
	key      crypto.Signer
	certFile string
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "transport test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	certFile := filepath.Join(t.TempDir(), "ca.pem")
	writePEM(t, certFile, "CERTIFICATE", der)
	return testCA{cert: cert, key: key, certFile: certFile}
}

func (ca testCA) issue(t *testing.T, dnsNames []string, ips []net.IP, client bool) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	if client {
		usage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "transport test peer"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writePEM(t, certFile, "CERTIFICATE", der)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, keyFile, "PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, kind string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func startTLSServer(t *testing.T, certFile, keyFile string, ca testCA, requireClient bool) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if requireClient {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = roots
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

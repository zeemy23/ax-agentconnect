// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// transportConfig is endpoint-local TLS configuration. An empty configuration
// is valid for HTTPS when the peer uses a public certificate; plaintext
// endpoints reject nonempty TLS settings.
type transportConfig struct {
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

type parsedEndpoint struct {
	address string
	host    string
	tls     bool
}

// parseEndpoint accepts only host:port or an http(s) URL with exactly that
// authority. In particular, a resolver URI must never reach grpc.NewClient.
func parseEndpoint(raw string) (parsedEndpoint, error) {
	if raw == "" {
		return parsedEndpoint{}, errors.New("address is required")
	}
	if raw != strings.TrimSpace(raw) || hasEndpointSpace(raw) {
		return parsedEndpoint{}, fmt.Errorf("address %q contains whitespace", raw)
	}

	address := raw
	secure := false
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return parsedEndpoint{}, fmt.Errorf("invalid address %q: %w", raw, err)
		}
		scheme := strings.ToLower(u.Scheme)
		if scheme != "http" && scheme != "https" {
			return parsedEndpoint{}, fmt.Errorf("address %q must use http or https", raw)
		}
		if u.Opaque != "" || u.Host == "" || u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || strings.Contains(raw, "#") || u.ForceQuery {
			return parsedEndpoint{}, fmt.Errorf("address %q must be an http(s) host:port URL without path, query, fragment, or userinfo", raw)
		}
		if strings.ContainsAny(u.Host, `\\/?#%`) {
			return parsedEndpoint{}, fmt.Errorf("address %q has an invalid host", raw)
		}
		address = u.Host
		secure = scheme == "https"
	} else if strings.Contains(raw, "://") || strings.ContainsAny(raw, `/\\?#@`) {
		return parsedEndpoint{}, fmt.Errorf("address %q must be host:port or an http(s) URL", raw)
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return parsedEndpoint{}, fmt.Errorf("address %q must be host:port", raw)
	}
	if host == "" {
		return parsedEndpoint{}, errors.New("address host is empty")
	}
	if !decimalPort(port) {
		return parsedEndpoint{}, fmt.Errorf("address %q has an invalid port", raw)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return parsedEndpoint{}, fmt.Errorf("address %q has a port outside 1-65535", raw)
	}
	if strings.ContainsAny(host, `%/\\?#@`) || hasEndpointControl(host) {
		return parsedEndpoint{}, fmt.Errorf("address %q has an invalid host", raw)
	}
	if strings.HasPrefix(address, "[") && net.ParseIP(host) == nil {
		return parsedEndpoint{}, fmt.Errorf("address %q has an invalid bracketed host", raw)
	}
	// Endpoint-local DNS zones make localhost pinning and TLS authority
	// handling ambiguous. Numeric IPv6 without a zone remains supported.
	if strings.Contains(host, "%") {
		return parsedEndpoint{}, fmt.Errorf("address %q must not use an IPv6 zone", raw)
	}
	if strings.HasPrefix(host, "[") || strings.HasSuffix(host, "]") {
		return parsedEndpoint{}, fmt.Errorf("address %q has an invalid host", raw)
	}

	return parsedEndpoint{
		address: net.JoinHostPort(host, strconv.Itoa(portNumber)),
		host:    host,
		tls:     secure,
	}, nil
}

func decimalPort(port string) bool {
	if port == "" {
		return false
	}
	for _, r := range port {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func hasEndpointSpace(value string) bool {
	for _, r := range value {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

func hasEndpointControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") || strings.EqualFold(host, "localhost.") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// validateEndpoint preserves the CLI's plaintext policy while leaving TLS
// policy to newConnection, where the endpoint-local files can be loaded.
func validateEndpoint(label, endpoint string, insecureInCluster bool) error {
	p, err := parseEndpoint(endpoint)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if !p.tls && !insecureInCluster && !isLoopbackHost(p.host) {
		return fmt.Errorf("%s %q is non-loopback; use --insecure-in-cluster to allow plaintext upstream gRPC", label, endpoint)
	}
	return nil
}

func newConnection(endpoint string, tlsConfig transportConfig, insecureInCluster bool, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	p, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if !p.tls && !insecureInCluster && !isLoopbackHost(p.host) {
		return nil, fmt.Errorf("plaintext endpoint %q is non-loopback; use --insecure-in-cluster to allow it", endpoint)
	}
	if !p.tls && tlsConfig != (transportConfig{}) {
		return nil, errors.New("TLS options require an https endpoint")
	}

	creds := credentials.TransportCredentials(insecure.NewCredentials())
	if p.tls {
		creds, err = tlsCredentials(tlsConfig)
		if err != nil {
			return nil, err
		}
	}

	// The explicit passthrough target prevents gRPC's default resolver from
	// interpreting operator input as dns://, xds://, unix://, or another scheme.
	opts := append([]grpc.DialOption(nil), extra...)
	opts = append(opts,
		grpc.WithTransportCredentials(creds),
		grpc.WithContextDialer(endpointDialer),
		grpc.WithDisableServiceConfig(),
		grpc.WithDisableRetry(),
	)
	if p.tls && tlsConfig.ServerName != "" {
		opts = append(opts, grpc.WithAuthority(tlsConfig.ServerName))
	}
	return grpc.NewClient("passthrough:///"+p.address, opts...)
}

func tlsCredentials(cfg transportConfig) (credentials.TransportCredentials, error) {
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, errors.New("TLS client certificate and key must be provided together")
	}
	if cfg.ServerName != "" && !validServerName(cfg.ServerName) {
		return nil, fmt.Errorf("invalid TLS server name %q", cfg.ServerName)
	}

	var roots *x509.CertPool
	if cfg.CAFile != "" {
		// A configured CA bundle is an explicit trust boundary for this
		// endpoint, so do not silently widen it with host roots.
		roots = x509.NewCertPool()
		pemBytes, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading TLS CA file %q: %w", cfg.CAFile, err)
		}
		if !roots.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("TLS CA file %q contains no certificates", cfg.CAFile)
		}
	} else {
		// A nil RootCAs value tells crypto/tls to use the host system roots. Use
		// the explicit pool when available so tests and callers can inspect the
		// same behavior across platforms.
		roots, _ = x509.SystemCertPool()
	}

	var certificates []tls.Certificate
	if cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading TLS client certificate: %w", err)
		}
		certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      roots,
		Certificates: certificates,
	}), nil
}

func validServerName(name string) bool {
	if name == "" || hasEndpointSpace(name) || hasEndpointControl(name) {
		return false
	}
	if net.ParseIP(name) != nil {
		return true
	}
	return !strings.ContainsAny(name, `/\\?#@:[]*`)
}

func endpointDialer(ctx context.Context, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(host, "localhost") && !strings.EqualFold(host, "localhost.") {
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, strings.TrimSuffix(host, "."))
	if err != nil {
		return nil, fmt.Errorf("resolving localhost: %w", err)
	}
	for _, ip := range ips {
		if !ip.IP.IsLoopback() {
			continue
		}
		conn, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(ip.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		err = dialErr
	}
	if err == nil {
		err = errors.New("localhost did not resolve to a loopback address")
	}
	return nil, err
}

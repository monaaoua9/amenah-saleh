package security

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server side: optional TLS plus an optional shared bearer token that every
// RPC (ingest and cluster alike) has to carry.
func ServerOptions(token, certFile, keyFile string) ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("tls cert and key must be given together")
	}
	if certFile != "" {
		c, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(c))
	}
	if token != "" {
		opts = append(opts,
			grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				if err := check(ctx, token); err != nil {
					return nil, err
				}
				return h(ctx, req)
			}),
			grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
				if err := check(ss.Context(), token); err != nil {
					return err
				}
				return h(srv, ss)
			}),
		)
	}
	return opts, nil
}

func check(ctx context.Context, token string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		got, ok := strings.CutPrefix(v, "Bearer ")
		if ok && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "missing or bad token")
}

type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

// plaintext is allowed on purpose: the token is a speed bump for a trusted
// network, TLS is what protects it on the wire
func (bearer) RequireTransportSecurity() bool { return false }

// tlsOn turns on TLS; caFile is optional and replaces the system roots (self-signed setups).
func DialOptions(token string, tlsOn bool, caFile string) ([]grpc.DialOption, error) {
	var opts []grpc.DialOption
	switch {
	case tlsOn || caFile != "":
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("no certificates in " + caFile)
			}
			cfg.RootCAs = pool
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	default:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearer(token)))
	}
	return opts, nil
}

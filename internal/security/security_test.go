package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/streamdb/internal/event"
	"github.com/monaaoua9/streamdb/internal/ingest"
)

type sub struct{}

func (sub) Submit(context.Context, string, []event.Event) error    { return nil }
func (sub) TrySubmit(context.Context, string, []event.Event) error { return nil }
func (sub) Committed(string) uint64                                { return 0 }

func selfSigned(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(k)
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	return
}

func start(t *testing.T, token, cert, key string) string {
	t.Helper()
	so, err := ServerOptions(token, cert, key)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(so...)
	ingest.NewServer(sub{}, false).Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

func try(t *testing.T, addr string, opts []grpc.DialOption) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := ingest.Dial(ctx, addr, opts...)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Send("p", []event.Event{{Key: "a", Seq: 1}}); err != nil {
		return err
	}
	_, err = c.Recv()
	return err
}

func TestToken(t *testing.T) {
	addr := start(t, "s3cret", "", "")

	good, _ := DialOptions("s3cret", false, "")
	if err := try(t, addr, good); err != nil {
		t.Fatalf("good token: %v", err)
	}
	bad, _ := DialOptions("nope", false, "")
	if c := status.Code(try(t, addr, bad)); c != codes.Unauthenticated {
		t.Fatalf("bad token: %v", c)
	}
	none, _ := DialOptions("", false, "")
	if c := status.Code(try(t, addr, none)); c != codes.Unauthenticated {
		t.Fatalf("no token: %v", c)
	}
}

func TestTLSWithToken(t *testing.T) {
	cert, key := selfSigned(t)
	addr := start(t, "tok", cert, key)

	ok, err := DialOptions("tok", true, cert)
	if err != nil {
		t.Fatal(err)
	}
	if err := try(t, addr, ok); err != nil {
		t.Fatalf("tls+token: %v", err)
	}
	// plaintext client against a TLS server must not get through
	plain, _ := DialOptions("tok", false, "")
	if err := try(t, addr, plain); err == nil {
		t.Fatal("plaintext accepted by tls server")
	}
}

func TestHalfConfiguredTLS(t *testing.T) {
	if _, err := ServerOptions("", "only-cert.pem", ""); err == nil {
		t.Fatal("want error")
	}
	if _, err := DialOptions("", false, filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("want error for missing ca")
	}
}

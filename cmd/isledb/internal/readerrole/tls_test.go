package readerrole

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string // the CA certificate, PEM
}

func newTestCA(t *testing.T, dir string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	file := filepath.Join(dir, "ca.pem")
	writePEM(t, file, "CERTIFICATE", der)
	return &testCA{cert: cert, key: key, file: file}
}

// issue signs a certificate for name, a server's or a client's, and writes it
// and its key to dir; it returns the two files and the loaded pair.
func (ca *testCA) issue(t *testing.T, dir, name string, usage x509.ExtKeyUsage) (certFile, keyFile string, pair tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, name+".pem"), filepath.Join(dir, name+"-key.pem")
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	pair, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, pair
}

func writePEM(t *testing.T, file, kind string, der []byte) {
	t.Helper()
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestReadCacheClientTrustsThePrivateCAAndPresentsItsCertificate runs a
// cache stand-in whose certificate a private CA signed and which requires a
// client certificate from that CA.
func TestReadCacheClientTrustsThePrivateCAAndPresentsItsCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, dir)
	_, _, serverPair := ca.issue(t, dir, "server", x509.ExtKeyUsageServerAuth)
	clientCert, clientKey, _ := ca.issue(t, dir, "client", x509.ExtKeyUsageClientAuth)
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverPair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	defer server.Close()

	get := func(c ReadCache) error {
		client, err := readCacheClient(c)
		if err != nil {
			t.Fatal(err)
		}
		if client == nil {
			client = &http.Client{}
		}
		defer client.CloseIdleConnections()
		resp, err := client.Get(server.URL)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	}
	if err := get(ReadCache{CAFile: ca.file, Cert: clientCert, Key: clientKey}); err != nil {
		t.Fatalf("with the CA and a client certificate: %v", err)
	}
	if err := get(ReadCache{CAFile: ca.file}); err == nil {
		t.Fatal("the cache accepted a client without a certificate")
	}
	if err := get(ReadCache{Cert: clientCert, Key: clientKey}); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("without the CA: %v, want the cache's certificate refused", err)
	}
}

func TestReadCacheClientRefusesBadFiles(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "not.pem")
	if err := os.WriteFile(notPEM, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t, dir)
	clientCert, _, _ := ca.issue(t, dir, "client", x509.ExtKeyUsageClientAuth)
	for _, c := range []struct {
		name string
		rc   ReadCache
		want string
	}{
		{"missing CA", ReadCache{CAFile: filepath.Join(dir, "none.pem")}, "read_cache.ca_file"},
		{"CA without certificates", ReadCache{CAFile: notPEM}, "holds no certificates"},
		{"certificate without its key", ReadCache{Cert: clientCert, Key: notPEM}, "read_cache.cert and key"},
	} {
		if _, err := readCacheClient(c.rc); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error naming %q", c.name, err, c.want)
		}
	}
	if client, err := readCacheClient(ReadCache{URL: "https://cache:8090"}); client != nil || err != nil {
		t.Fatalf("no TLS settings: %v %v, want the reader's own client", client, err)
	}
}

func TestReadCacheTokenIsOneLine(t *testing.T) {
	dir := t.TempDir()
	file := func(name, body string) string {
		f := filepath.Join(dir, name)
		if err := os.WriteFile(f, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return f
	}
	if tok, err := readCacheToken(file("one", "# for the eu cache\n\n  secret  \n")); err != nil || tok != "secret" {
		t.Fatalf("one token among comments: %q %v", tok, err)
	}
	for _, c := range []struct{ name, body, want string }{
		{"two", "a\nb\n", "holds 2 tokens"},
		{"empty", "\n# none yet\n", "holds 0 tokens"},
	} {
		if _, err := readCacheToken(file(c.name, c.body)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	if _, err := readCacheToken(filepath.Join(dir, "missing")); err == nil || !strings.Contains(err.Error(), "read_cache.token_file") {
		t.Fatalf("missing file: %v", err)
	}
}

// TestReadCacheWithoutCredentialsWarns: a cache URL with neither a token nor
// a certificate works only against an insecure cache, so the reader says so.
func TestReadCacheWithoutCredentialsWarns(t *testing.T) {
	for _, c := range []struct {
		name  string
		token bool
		warn  bool
	}{{"no credentials", false, true}, {"a token", true, false}} {
		t.Run(c.name, func(t *testing.T) {
			cfg := NewConfig().(*Config)
			cfg.Prefix, cfg.CacheDir = "prod/a", t.TempDir()
			cfg.ReadCache.URL = "http://127.0.0.1:1"
			if c.token {
				cfg.ReadCache.TokenFile = filepath.Join(t.TempDir(), "token")
				if err := os.WriteFile(cfg.ReadCache.TokenFile, []byte("secret\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var logs strings.Builder
			log := slog.New(slog.NewTextHandler(&logs, nil))
			svc, err := cfg.Start(context.Background(), service.Common{BucketURL: "file://" + t.TempDir()}, log)
			if err != nil {
				t.Fatal(err)
			}
			_ = svc.Close()
			if got := strings.Contains(logs.String(), "no token_file or cert"); got != c.warn {
				t.Fatalf("warned %t, want %t: %s", got, c.warn, logs.String())
			}
		})
	}
}

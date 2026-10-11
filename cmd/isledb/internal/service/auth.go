package service

import (
	"bufio"
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
)

// TokenAuth checks bearer tokens against a file of them, which Reload
// rereads.
type TokenAuth struct {
	path   string
	tokens atomic.Pointer[[][]byte]
}

func NewTokenAuth(path string) (*TokenAuth, error) {
	a := &TokenAuth{path: path}
	return a, a.Reload()
}

// Reload rereads the file. On an error the tokens in use stay.
func (a *TokenAuth) Reload() error {
	data, err := os.ReadFile(a.path)
	if err != nil {
		return err
	}
	var tokens [][]byte
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			tokens = append(tokens, []byte(line))
		}
	}
	if len(tokens) == 0 {
		return fmt.Errorf("%s holds no tokens", a.path)
	}
	a.tokens.Store(&tokens)
	return nil
}

// Allows reports whether r carries a token from the file.
func (a *TokenAuth) Allows(r *http.Request) bool {
	given, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	allowed := false
	for _, t := range *a.tokens.Load() {
		if subtle.ConstantTimeCompare([]byte(given), t) == 1 {
			allowed = true
		}
	}
	return allowed
}

// Wrap answers 401 to requests it does not allow.
func (a *TokenAuth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.Allows(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// serverTLS returns the TLS configuration, or nil without a certificate.
// With a client CA, clients must present a certificate it signed.
func serverTLS(c TLSConfig) (*tls.Config, error) {
	if c.Cert == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.Cert, c.Key)
	if err != nil {
		return nil, err
	}
	conf := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if c.ClientCA != "" {
		pem, err := os.ReadFile(c.ClientCA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New(c.ClientCA + " holds no certificates")
		}
		conf.ClientCAs = pool
		conf.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return conf, nil
}

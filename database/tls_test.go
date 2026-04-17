package database

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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"db-ferry/config"
)

func generateTestCert(t *testing.T, notAfter time.Time) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key error = %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate error = %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file error = %v", err)
	}
	_ = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	_ = certFile.Close()

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key error = %v", err)
	}
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file error = %v", err)
	}
	_ = pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	_ = keyFile.Close()

	return certPath, keyPath
}

func TestValidateTLSConfigMissingFile(t *testing.T) {
	err := ValidateTLSConfig(config.DatabaseConfig{
		SSLMode: config.SSLModeRequire,
		SSLCert: "/nonexistent/path/cert.pem",
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected file not found error, got %v", err)
	}
}

func TestValidateTLSConfigExpiredCert(t *testing.T) {
	certPath, _ := generateTestCert(t, time.Now().Add(-time.Hour))
	err := ValidateTLSConfig(config.DatabaseConfig{
		SSLMode: config.SSLModeRequire,
		SSLCert: certPath,
	})
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired certificate error, got %v", err)
	}
}

func TestValidateTLSConfigValidCert(t *testing.T) {
	certPath, _ := generateTestCert(t, time.Now().Add(24*time.Hour))
	err := ValidateTLSConfig(config.DatabaseConfig{
		SSLMode: config.SSLModeRequire,
		SSLCert: certPath,
	})
	if err != nil {
		t.Fatalf("expected valid certificate to pass, got %v", err)
	}
}

func TestTestTLSHandshakeSkipsFileDBs(t *testing.T) {
	for _, dbType := range []string{config.DatabaseTypeSQLite, config.DatabaseTypeDuckDB} {
		err := TestTLSHandshake(config.DatabaseConfig{
			Type:    dbType,
			SSLMode: config.SSLModeRequire,
		})
		if err != nil {
			t.Fatalf("TestTLSHandshake should skip %s, got %v", dbType, err)
		}
	}
}

func TestTestTLSHandshakeSuccess(t *testing.T) {
	certPath, keyPath := generateTestCert(t, time.Now().Add(24*time.Hour))

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{mustLoadCert(t, certPath, keyPath)},
	})
	if err != nil {
		t.Fatalf("tls listen error = %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		tlsConn := conn.(*tls.Conn)
		_ = tlsConn.Handshake()
		_ = tlsConn.Close()
	}()

	addr := listener.Addr().(*net.TCPAddr)
	err = TestTLSHandshake(config.DatabaseConfig{
		Type:    config.DatabaseTypePostgreSQL,
		Host:    "127.0.0.1",
		Port:    strconv.Itoa(addr.Port),
		SSLMode: config.SSLModeRequire,
	})
	if err != nil {
		t.Fatalf("expected TLS handshake to succeed in require mode, got %v", err)
	}
}

func mustLoadCert(t *testing.T, certPath, keyPath string) tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load x509 key pair error = %v", err)
	}
	return cert
}

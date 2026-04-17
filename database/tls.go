package database

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"db-ferry/config"

	"github.com/go-sql-driver/mysql"
)

var (
	mysqlTLSRegistryMutex sync.Mutex
	mysqlTLSRegistryCount int
)

// ValidateTLSConfig checks TLS-related files and certificate validity.
func ValidateTLSConfig(dbCfg config.DatabaseConfig) error {
	if dbCfg.SSLMode == config.SSLModeDisable || dbCfg.SSLMode == "" {
		return nil
	}

	checkFile := func(path, name string) error {
		if path == "" {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("%s not found: %w", name, err)
		}
		if info.IsDir() {
			return fmt.Errorf("%s must be a file, not a directory", name)
		}
		return nil
	}

	if err := checkFile(dbCfg.SSLCert, "ssl_cert"); err != nil {
		return err
	}
	if err := checkFile(dbCfg.SSLKey, "ssl_key"); err != nil {
		return err
	}
	if err := checkFile(dbCfg.SSLRootCert, "ssl_root_cert"); err != nil {
		return err
	}

	if dbCfg.SSLCert != "" {
		certPEM, err := os.ReadFile(dbCfg.SSLCert)
		if err != nil {
			return fmt.Errorf("failed to read ssl_cert: %w", err)
		}
		block, _ := pem.Decode(certPEM)
		if block == nil {
			return fmt.Errorf("ssl_cert does not contain a valid PEM certificate")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse ssl_cert: %w", err)
		}
		if time.Now().After(cert.NotAfter) {
			return fmt.Errorf("ssl_cert has expired (not after %s)", cert.NotAfter.Format(time.RFC3339))
		}
	}

	return nil
}

// TestTLSHandshake attempts a raw TLS dial to validate handshake.
func TestTLSHandshake(dbCfg config.DatabaseConfig) error {
	if dbCfg.SSLMode == config.SSLModeDisable || dbCfg.SSLMode == "" {
		return nil
	}

	switch dbCfg.Type {
	case config.DatabaseTypeSQLite, config.DatabaseTypeDuckDB:
		return nil
	}

	tlsConfig, err := buildTLSConfig(dbCfg)
	if err != nil {
		return fmt.Errorf("failed to build TLS config: %w", err)
	}

	addr := net.JoinHostPort(dbCfg.Host, dbCfg.Port)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}
	_ = conn.Close()
	return nil
}

// registerMySQLTLSConfig registers a tls.Config for the MySQL driver and returns its name.
func registerMySQLTLSConfig(dbCfg config.DatabaseConfig) (string, error) {
	if dbCfg.SSLMode == config.SSLModeDisable || dbCfg.SSLMode == "" {
		return "", nil
	}

	tlsConfig, err := buildTLSConfig(dbCfg)
	if err != nil {
		return "", err
	}

	mysqlTLSRegistryMutex.Lock()
	defer mysqlTLSRegistryMutex.Unlock()
	mysqlTLSRegistryCount++
	name := fmt.Sprintf("db-ferry-%d", mysqlTLSRegistryCount)
	if err := mysql.RegisterTLSConfig(name, tlsConfig); err != nil {
		return "", fmt.Errorf("failed to register MySQL TLS config: %w", err)
	}
	return name, nil
}

func buildTLSConfig(dbCfg config.DatabaseConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	switch dbCfg.SSLMode {
	case config.SSLModeRequire:
		tlsConfig.InsecureSkipVerify = true
	case config.SSLModeVerifyCA:
		tlsConfig.InsecureSkipVerify = false
	case config.SSLModeVerifyFull:
		tlsConfig.InsecureSkipVerify = false
		tlsConfig.ServerName = dbCfg.Host
	default:
		return nil, fmt.Errorf("unsupported ssl_mode '%s' for TLS", dbCfg.SSLMode)
	}

	if dbCfg.SSLRootCert != "" {
		caPEM, err := os.ReadFile(dbCfg.SSLRootCert)
		if err != nil {
			return nil, fmt.Errorf("failed to read ssl_root_cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse ssl_root_cert")
		}
		tlsConfig.RootCAs = pool
	}

	if dbCfg.SSLCert != "" && dbCfg.SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(dbCfg.SSLCert, dbCfg.SSLKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

package grpcapi

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

	"github.com/tcw/ibsen/adapter/driven/blockstore/filestore"
	"github.com/tcw/ibsen/core/manager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// writeSelfSignedCert writes a certificate for localhost and its key to dir and returns a
// pool that trusts the certificate.
func writeSelfSignedCert(t *testing.T, dir string) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDer, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer}), 0600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	pool.AddCert(cert)
	return certFile, keyFile, pool
}

// relativeToWorkingDir returns path relative to the working directory.
func relativeToWorkingDir(t *testing.T, path string) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(wd, path)
	if err != nil {
		t.Skipf("no relative path from %s to %s: %v", wd, path, err)
	}
	return rel
}

func TestSecureServer_loadsCertificateFromRelativePaths(t *testing.T) {
	certFile, keyFile, pool := writeSelfSignedCert(t, t.TempDir())
	sec := GRPCSecurity{
		CertKeyFile:   relativeToWorkingDir(t, certFile),
		PrivteKeyFile: relativeToWorkingDir(t, keyFile),
	}
	if filepath.IsAbs(sec.CertKeyFile) || filepath.IsAbs(sec.PrivteKeyFile) {
		t.Fatalf("paths %s and %s are not relative", sec.CertKeyFile, sec.PrivteKeyFile)
	}

	logManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{Store: filestore.NewOS(t.TempDir()), MaxBlockSize: 2000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(logManager.Close)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	igs := NewSecureIbsenGrpcServer(&logManager, sec, time.Minute, time.Millisecond)
	served := make(chan error, 1)
	go func() {
		served <- igs.StartGRPC(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialed := make(chan struct{})
	var conn *grpc.ClientConn
	var dialErr error
	go func() {
		defer close(dialed)
		conn, dialErr = DialContext(ctx, lis.Addr().String(),
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, "localhost")))
	}()
	select {
	case err := <-served:
		cancel()
		<-dialed
		t.Fatalf("secure server did not start: %v", err)
	case <-dialed:
	}
	if dialErr != nil {
		t.Fatalf("unable to connect over TLS: %v", dialErr)
	}
	if _, err := NewIbsenClient(conn).List(ctx, &EmptyArgs{}); err != nil {
		t.Fatalf("List over TLS failed: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	_ = lis.Close()
	<-served
}

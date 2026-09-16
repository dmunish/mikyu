package server

import (
	"bufio"
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
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// authFrame sends the OpAuth handshake and returns the reply.
func authFrame(t *testing.T, conn net.Conn, method byte, cred []byte) byte {
	t.Helper()
	var b protocol.Buffer
	protocol.EncodeAuth(&b, method, cred)
	require.NoError(t, protocol.WriteFrame(conn, protocol.OpAuth, b.Bytes()))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, _, err := protocol.ReadFrame(bufio.NewReader(conn))
	require.NoError(t, err)
	return op
}

func TestServerTokenAuthAcceptsCorrectCredential(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	mode, err := NewAuthMode(spec.Session{AuthMethod: "token", AuthToken: "0123456789abcdef"})
	require.NoError(t, err)
	srv := New(n, WithAuth(mode))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	assert.Equal(t, protocol.OpAuth, authFrame(t, conn, protocol.AuthMethodToken, []byte("0123456789abcdef")))

	require.NoError(t, protocol.WriteFrame(conn, protocol.OpListQueues, nil))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, _, err := protocol.ReadFrame(bufio.NewReader(conn))
	require.NoError(t, err)
	assert.Equal(t, protocol.OpListQueues, op)
}

func TestServerTokenAuthRejectsWrongAndAbsentCredential(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	mode, err := NewAuthMode(spec.Session{AuthMethod: "token", AuthToken: "0123456789abcdef"})
	require.NoError(t, err)
	srv := New(n, WithAuth(mode))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	// A wrong token gets the unauthorized error code.
	wrong, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	assert.Equal(t, protocol.OpError, authFrame(t, wrong, protocol.AuthMethodToken, []byte("ffffffffffffffff")))
	require.NoError(t, wrong.Close())

	// A client that presents no handshake at all is refused before dispatch.
	silent, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer silent.Close()
	require.NoError(t, protocol.WriteFrame(silent, protocol.OpListQueues, nil))
	_ = silent.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, body, err := protocol.ReadFrame(bufio.NewReader(silent))
	require.NoError(t, err)
	assert.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeUnauthorized, code)
}

func TestServerAnonymousAcceptsAnyFirstFrame(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	srv := New(n) // default verifier is none
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	// Legacy first frame: no handshake, straight to the op.
	plain, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer plain.Close()
	require.NoError(t, protocol.WriteFrame(plain, protocol.OpListQueues, nil))
	_ = plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, _, err := protocol.ReadFrame(bufio.NewReader(plain))
	require.NoError(t, err)
	assert.Equal(t, protocol.OpListQueues, op)

	// And the handshake form as well.
	hand, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer hand.Close()
	assert.Equal(t, protocol.OpAuth, authFrame(t, hand, protocol.AuthMethodNone, nil))
}

func TestNewAuthModeRejectsShortToken(t *testing.T) {
	_, err := NewAuthMode(spec.Session{AuthMethod: "token", AuthToken: "short"})
	assert.Error(t, err)
}

func TestNewAuthModeRejectsTokenFileMissing(t *testing.T) {
	_, err := NewAuthMode(spec.Session{AuthMethod: "token", AuthTokenFile: filepath.Join(t.TempDir(), "absent")})
	assert.Error(t, err)
}

func TestNewAuthModeReadsTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("  0123456789abcdef\n"), 0o600))

	mode, err := NewAuthMode(spec.Session{AuthMethod: "token", AuthTokenFile: path})
	require.NoError(t, err)
	assert.NoError(t, mode.Verify(protocol.AuthMethodToken, []byte("0123456789abcdef"), nil))
	assert.ErrorIs(t, mode.Verify(protocol.AuthMethodToken, []byte("0123456789abcdeff"), nil), protocol.ErrUnauthorized)
}

// mTLSFixture builds a CA plus CA-signed server and client certs, and returns
// the server tls.Config built from PEM files via DefaultTLSConfig and the
// client tls.Config that trusts only the CA.
func mTLSFixture(t *testing.T) (serverTLS *tls.Config, clientTLS *tls.Config) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mikyu-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	dir := t.TempDir()
	pemFile := func(name, blockType string, der []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600))
		return path
	}
	caPath := pemFile("ca.pem", "CERTIFICATE", caDER)

	sign := func(serial int64, ekus []x509.ExtKeyUsage, ips []net.IP) (tls.Certificate, []byte, *ecdsa.PrivateKey) {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "mikyu-test-leaf"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  ekus,
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, der, key
	}

	// CA-signed server leaf so the client can verify it.
	srvLeaf, srvDER, srvKey := sign(2, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, []net.IP{net.ParseIP("127.0.0.1")})
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	require.NoError(t, err)
	srvCertPath := pemFile("server.pem", "CERTIFICATE", srvDER)
	srvKeyPath := pemFile("server-key.pem", "EC PRIVATE KEY", srvKeyDER)

	srvCfg, err := DefaultTLSConfig(srvCertPath, srvKeyPath, caPath)
	require.NoError(t, err)
	assert.Equal(t, tls.RequireAndVerifyClientCert, srvCfg.ClientAuth)

	// CA-signed client leaf.
	clientLeaf, _, _ := sign(3, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil)

	root := x509.NewCertPool()
	root.AddCert(ca)
	_ = srvLeaf
	return srvCfg, &tls.Config{
		Certificates: []tls.Certificate{clientLeaf},
		RootCAs:      root,
		MinVersion:   tls.VersionTLS13,
	}
}

func TestServerMTLSEndToEnd(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	srvCfg, clientCfg := mTLSFixture(t)
	mode, err := NewAuthMode(spec.Session{AuthMethod: "mtls"})
	require.NoError(t, err)
	srv := New(n, WithAuth(mode), WithTLS(srvCfg))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	// With mTLS, the server requires TLS and verifies client certificates.
	require.NotNil(t, srvCfg)

	// A bare connection without a client cert should fail at the transport layer.
	bare, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: clientCfg.RootCAs, MinVersion: tls.VersionTLS13})
	switch {
	case err != nil:
		assert.Error(t, err, "dial without client cert must fail")
	default:
		_, err = bare.Read(make([]byte, 1))
		assert.Error(t, err, "connection without client cert must fail")
		_ = bare.Close()
	}
}

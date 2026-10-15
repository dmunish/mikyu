package engine

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

const (
	masterKeySize   = 32
	masterKeyIDSize = 8
)

// MasterKey is one AES-256 key from a key file; ID derives from the key material and travels with wrapped data keys.
type MasterKey struct {
	ID  [masterKeyIDSize]byte
	Key [masterKeySize]byte
}

// MasterKeySet holds the master keys loaded from a key file; the last entry wraps new data keys.
type MasterKeySet []MasterKey

// LoadMasterKeys reads one hex-encoded 32-byte key per line; blank lines and # comments are skipped.
func LoadMasterKeys(path string) (MasterKeySet, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptionKey, err)
	}
	defer f.Close()
	if runtime.GOOS != "windows" {
		if info, serr := f.Stat(); serr == nil && info.Mode().Perm()&0o077 != 0 {
			slog.Warn("encryption key file should not be readable by group or others", "path", path)
		}
	}
	keys, err := parseMasterKeys(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return keys, nil
}

func parseMasterKeys(r io.Reader) (MasterKeySet, error) {
	var keys MasterKeySet
	scan := bufio.NewScanner(r)
	for line := 1; scan.Scan(); line++ {
		text := strings.TrimSpace(scan.Text())
		if text == "" || text[0] == '#' {
			continue
		}
		raw, err := hex.DecodeString(text)
		if err != nil || len(raw) != masterKeySize {
			return nil, fmt.Errorf("%w: line %d is not a 32-byte hex key", ErrEncryptionKey, line)
		}
		var k MasterKey
		copy(k.Key[:], raw)
		sum := sha256.Sum256(raw)
		copy(k.ID[:], sum[:masterKeyIDSize])
		keys = append(keys, k)
	}
	if err := scan.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptionKey, err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: key file holds no keys", ErrEncryptionKey)
	}
	return keys, nil
}

// wrap seals data under the newest master key with aad bound, prefixing the key ID.
func (s MasterKeySet) wrap(aad, data []byte) ([]byte, error) {
	if len(s) == 0 {
		return nil, ErrEncryptionKey
	}
	return s[len(s)-1].wrap(aad, data)
}

// unwrap opens data wrapped by the master key whose ID prefixes it.
func (s MasterKeySet) unwrap(aad, wrapped []byte) ([]byte, error) {
	if len(s) == 0 || len(wrapped) < masterKeyIDSize {
		return nil, ErrEncryptionKey
	}
	var id [masterKeyIDSize]byte
	copy(id[:], wrapped)
	for _, k := range s {
		if k.ID != id {
			continue
		}
		pt, err := k.open(aad, wrapped[masterKeyIDSize:])
		if err != nil {
			return nil, fmt.Errorf("%w: data key does not open under master %x", ErrEncryptionKey, id)
		}
		return pt, nil
	}
	return nil, fmt.Errorf("%w: no master key matches id %x", ErrEncryptionKey, id)
}

func (k MasterKey) wrap(aad, data []byte) ([]byte, error) {
	gcm, err := k.randomNonceGCM()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, masterKeyIDSize+len(data)+gcm.Overhead())
	out = append(out, k.ID[:]...)
	return gcm.Seal(out, nil, data, aad), nil
}

func (k MasterKey) open(aad, data []byte) ([]byte, error) {
	gcm, err := k.randomNonceGCM()
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, nil, data, aad)
}

func (k MasterKey) randomNonceGCM() (cipher.AEAD, error) {
	block, err := aes.NewCipher(k.Key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

// newDataKey draws a fresh AES-256 data key.
func newDataKey() ([masterKeySize]byte, error) {
	var key [masterKeySize]byte
	_, err := rand.Read(key[:])
	return key, err
}

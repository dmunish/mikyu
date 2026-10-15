package engine

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

const (
	keysFileName = "keys.dat"
	keysTmpName  = "keys.dat.tmp"
	keysMagic    = "MKYK"
	keysVersion  = 1

	// gcmOverhead is what one sealed payload grows by: the GCM tag plus the
	// random nonce cipher.NewGCMWithRandomNonce prepends to every Seal output.
	gcmOverhead = 28
)

// queueCipher seals and opens record payloads for one queue across its data-key
// generations. A generation is one data key; records sealed under older
// generations stay readable, and decryption dispatches by trying each AEAD in
// age order — the newest usually seals, so older generations only pay for
// pre-rotation records.
type queueCipher struct {
	gens []keyGen
}

type keyGen struct {
	aead    cipher.AEAD
	wrapped []byte // id-prefixed master wrap, exactly as stored in keys.dat
}

// openQueueCipher loads the queue's data keys, or mints the first generation
// for a fresh queue. hasData reports whether the queue already holds records,
// in which case a missing key store is unrecoverable.
func openQueueCipher(root *os.Root, name string, masters MasterKeySet, hasData bool) (*queueCipher, error) {
	raw, err := root.ReadFile(keysFileName)
	switch {
	case err == nil:
		return decodeKeyStore(name, masters, raw)
	case errors.Is(err, fs.ErrNotExist) && hasData:
		return nil, fmt.Errorf("%w: queue %q holds sealed records but has no %s", ErrEncryptionKey, name, keysFileName)
	case errors.Is(err, fs.ErrNotExist):
		return mintQueueCipher(root, name, masters)
	default:
		return nil, err
	}
}

// mintQueueCipher draws a data key, wraps it under the newest master with the
// queue name bound, and persists the key store before any record exists.
func mintQueueCipher(root *os.Root, name string, masters MasterKeySet) (*queueCipher, error) {
	if len(masters) == 0 {
		return nil, fmt.Errorf("%w: queue %q wants sealed payloads but no master keys are loaded", ErrEncryptionKey, name)
	}
	key, err := newDataKey()
	if err != nil {
		return nil, err
	}
	wrapped, err := masters.wrap([]byte(name), key[:])
	if err != nil {
		return nil, err
	}
	aead, err := newPayloadAEAD(key[:])
	if err != nil {
		return nil, err
	}
	c := &queueCipher{gens: []keyGen{{aead: aead, wrapped: wrapped}}}
	if err := c.save(root); err != nil {
		return nil, err
	}
	return c, nil
}

// rotate seals new records under a fresh data key generation wrapped by the
// newest master; existing generations stay readable.
func (c *queueCipher) rotate(root *os.Root, name string, masters MasterKeySet) error {
	if len(masters) == 0 {
		return fmt.Errorf("%w: queue %q cannot rotate without master keys", ErrEncryptionKey, name)
	}
	key, err := newDataKey()
	if err != nil {
		return err
	}
	wrapped, err := masters.wrap([]byte(name), key[:])
	if err != nil {
		return err
	}
	aead, err := newPayloadAEAD(key[:])
	if err != nil {
		return err
	}
	c.gens = append(c.gens, keyGen{aead: aead, wrapped: wrapped})
	return c.save(root)
}

// seal encrypts plaintext into dst's free capacity. The random nonce is
// prepended, so dst must have room for len(plaintext)+gcmOverhead bytes.
func (c *queueCipher) seal(dst, plaintext []byte) {
	g := &c.gens[len(c.gens)-1]
	g.aead.Seal(dst[:0], nil, plaintext, nil)
}

// open decrypts ciphertext under the first generation whose key authenticates it.
func (c *queueCipher) open(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < gcmOverhead {
		return nil, ErrCorruptRecord
	}
	for i := range c.gens {
		pt, err := c.gens[i].aead.Open(nil, nil, ciphertext, nil)
		if err == nil {
			return pt, nil
		}
	}
	return nil, fmt.Errorf("%w: payload does not open under any loaded key generation", ErrCorruptRecord)
}

// save atomically rewrites keys.dat; the queue is unreadable without it, so the
// rename is followed by a directory sync.
func (c *queueCipher) save(root *os.Root) error {
	f, err := root.OpenFile(keysTmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(keysMagic)+5+64*len(c.gens))
	buf = append(buf, keysMagic...)
	buf = append(buf, keysVersion)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(c.gens)))
	buf = append(buf, count[:]...)
	for _, g := range c.gens {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(g.wrapped)))
		buf = append(buf, size[:]...)
		buf = append(buf, g.wrapped...)
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := root.Rename(keysTmpName, keysFileName); err != nil {
		return err
	}
	return syncDir(root)
}

// decodeKeyStore unwraps every stored generation under the loaded masters.
func decodeKeyStore(name string, masters MasterKeySet, raw []byte) (*queueCipher, error) {
	if len(raw) < 9 || string(raw[:4]) != keysMagic {
		return nil, fmt.Errorf("%w: %s is not a mikyu key store", ErrEncryptionKey, keysFileName)
	}
	if raw[4] != keysVersion {
		return nil, fmt.Errorf("%w: %s version %d is too new", ErrEncryptionKey, keysFileName, raw[4])
	}
	count := binary.BigEndian.Uint32(raw[5:9])
	body := raw[9:]
	c := &queueCipher{gens: make([]keyGen, 0, count)}
	for range count {
		if len(body) < 4 {
			return nil, fmt.Errorf("%w: %s ends mid-entry", ErrEncryptionKey, keysFileName)
		}
		size := binary.BigEndian.Uint32(body[:4])
		body = body[4:]
		if uint32(len(body)) < size {
			return nil, fmt.Errorf("%w: %s ends mid-entry", ErrEncryptionKey, keysFileName)
		}
		wrapped := body[:size]
		body = body[size:]
		key, err := masters.unwrap([]byte(name), wrapped)
		if err != nil {
			return nil, err
		}
		aead, err := newPayloadAEAD(key)
		if err != nil {
			return nil, err
		}
		c.gens = append(c.gens, keyGen{aead: aead, wrapped: append([]byte(nil), wrapped...)})
	}
	if len(c.gens) == 0 {
		return nil, fmt.Errorf("%w: %s holds no data keys", ErrEncryptionKey, keysFileName)
	}
	return c, nil
}

func newPayloadAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

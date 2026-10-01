package bench

import (
	"bytes"
	"encoding/binary"
)

// payloadFor fills dst with the deterministic payload for offset; the fnv64a header and splitmix64 body are contractual.
func payloadFor(seed uint32, offset uint64, dst []byte) {
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], offset)
	var sum [8]byte
	binary.BigEndian.PutUint64(sum[:], fnv1a(header[:]))
	copy(dst, sum[:])
	if len(dst) <= 8 {
		return
	}
	state := bodySeed(seed, offset)
	for i := 8; i < len(dst); i += 8 {
		var word [8]byte
		binary.BigEndian.PutUint64(word[:], splitmix64Next(&state))
		copy(dst[i:], word[:])
	}
}

// verifyPayload reports whether payload matches what payloadFor would produce for offset.
func verifyPayload(seed uint32, offset uint64, payload []byte) error {
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], offset)
	var sum [8]byte
	binary.BigEndian.PutUint64(sum[:], fnv1a(header[:]))
	if head := min(len(payload), 8); !bytes.Equal(payload[:head], sum[:head]) {
		return &VerifyError{Offset: offset, Reason: "payload differs from the seeded value"}
	}
	if len(payload) <= 8 {
		return nil
	}
	state := bodySeed(seed, offset)
	for i := 8; i < len(payload); i += 8 {
		var word [8]byte
		binary.BigEndian.PutUint64(word[:], splitmix64Next(&state))
		end := min(i+8, len(payload))
		if !bytes.Equal(payload[i:end], word[:end-i]) {
			return &VerifyError{Offset: offset, Reason: "payload differs from the seeded value"}
		}
	}
	return nil
}

// bodySeed mixes the seed and offset into the SplitMix64 starting state.
func bodySeed(seed uint32, offset uint64) uint64 {
	return fnv1a(binary.BigEndian.AppendUint64(nil, offset)) ^ (uint64(seed) << 32) ^ 0x9E3779B97F4A7C15
}

// splitmix64Next advances the SplitMix64 generator and returns its output.
func splitmix64Next(state *uint64) uint64 {
	*state += 0x9E3779B97F4A7C15
	z := *state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// fnv1a is the standard FNV-1a 64-bit hash, inlined to avoid an allocation per verification.
func fnv1a(data []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range data {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}

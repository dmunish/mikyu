package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeAuthRoundTrip(t *testing.T) {
	cases := []struct {
		method     byte
		credential []byte
	}{
		{AuthMethodNone, nil},
		{AuthMethodToken, []byte("0123456789abcdef")},
		{AuthMethodMTLS, nil},
	}
	for _, tc := range cases {
		var b Buffer
		EncodeAuth(&b, tc.method, tc.credential)

		method, cred, err := DecodeAuth(NewReader(b.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, tc.method, method)
		if tc.credential == nil {
			assert.Empty(t, cred)
		} else {
			assert.Equal(t, tc.credential, cred)
		}
	}
}

func TestDecodeAuthRejectsEmptyBody(t *testing.T) {
	_, _, err := DecodeAuth(NewReader(nil))
	assert.ErrorIs(t, err, ErrTruncatedBody)
}

func TestDecodeAuthRejectsTruncatedCredential(t *testing.T) {
	_, _, err := DecodeAuth(NewReader([]byte{AuthMethodToken, 0x00, 0x00}))
	assert.ErrorIs(t, err, ErrTruncatedBody)
}

func TestDecodeAuthConsumesExactlyCredential(t *testing.T) {
	trailing := uint16(0xAABB)
	var b Buffer
	EncodeAuth(&b, AuthMethodToken, []byte("0123456789abcdef"))
	body := append(b.Bytes(), byte(trailing>>8), byte(trailing))

	r := NewReader(body)
	method, cred, err := DecodeAuth(r)
	require.NoError(t, err)
	assert.Equal(t, AuthMethodToken, method)
	assert.Equal(t, "0123456789abcdef", string(cred))
	assert.Equal(t, 2, len(body)-r.i, "DecodeAuth must stop at the credential boundary")

	left, err := r.ReadU16()
	require.NoError(t, err)
	assert.Equal(t, trailing, left, "bytes after the credential stay in the body")
}

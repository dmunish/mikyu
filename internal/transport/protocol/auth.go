package protocol

// Auth methods a client declares in the OpAuth handshake frame.
const (
	AuthMethodNone  byte = 0
	AuthMethodToken byte = 1
	AuthMethodMTLS  byte = 2
)

// EncodeAuth writes a u8 method followed by a length-prefixed credential.
func EncodeAuth(b *Buffer, method byte, credential []byte) {
	b.b = append(b.b, method)
	b.WriteBytes(credential)
}

// DecodeAuth reads a u8 method and a length-prefixed credential.
func DecodeAuth(r *Reader) (byte, []byte, error) {
	if r.i+1 > len(r.b) {
		return 0, nil, ErrTruncatedBody
	}
	method := r.b[r.i]
	r.i++
	cred, err := r.ReadBytes()
	if err != nil {
		return 0, nil, err
	}
	return method, cred, nil
}

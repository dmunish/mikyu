package spec

// Durability selects the append acknowledgement policy.
type Durability string

const (
	DurabilitySync  Durability = "sync"
	DurabilityAsync Durability = "async"
)

// String returns the constant text.
func (d Durability) String() string { return string(d) }

// Valid reports whether d is one of the declared constants.
func (d Durability) Valid() bool {
	switch d {
	case DurabilitySync, DurabilityAsync:
		return true
	}
	return false
}

// Encryption selects at-rest protection for queue files.
type Encryption string

const (
	EncryptionNone Encryption = "none"
	EncryptionOS   Encryption = "os"
	EncryptionAES  Encryption = "aes"
)

// String returns the constant text.
func (e Encryption) String() string { return string(e) }

// Valid reports whether e is one of the declared constants.
func (e Encryption) Valid() bool {
	switch e {
	case EncryptionNone, EncryptionOS, EncryptionAES:
		return true
	}
	return false
}

// Compaction selects how closed segments shed superseded records.
type Compaction string

const (
	CompactionNone Compaction = "none"
	CompactionKeys Compaction = "keys"
)

// String returns the constant text.
func (c Compaction) String() string { return string(c) }

// Valid reports whether c is one of the declared constants.
func (c Compaction) Valid() bool {
	switch c {
	case CompactionNone, CompactionKeys:
		return true
	}
	return false
}

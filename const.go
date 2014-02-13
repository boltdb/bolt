package bolt

const version = 1

const (
	// MaxBucketNameSize is the maximum length of a bucket name, in bytes.
	MaxBucketNameSize = 255

	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = 4294967295
)

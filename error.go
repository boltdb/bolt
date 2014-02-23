package bolt

var (
	// ErrInvalid is returned when a data file is not a Bolt-formatted database.
	ErrInvalid = &Error{"Invalid database", nil}

	// ErrVersionMismatch is returned when the data file was created with a 
	// different version of Bolt.
	ErrVersionMismatch = &Error{"version mismatch", nil}

	// ErrDatabaseNotOpen is returned when a DB instance is accessed before it
	// is opened or after it is closed.
	ErrDatabaseNotOpen = &Error{"database not open", nil}

	// ErrDatabaseOpen is returned when opening a database that is
	// already open.
	ErrDatabaseOpen = &Error{"database already open", nil}

	// ErrBucketNotFound is returned when trying to access a bucket that has
	// not been created yet.
	ErrBucketNotFound = &Error{"bucket not found", nil}

	// ErrBucketExists is returned when creating a bucket that already exists.
	ErrBucketExists = &Error{"bucket already exists", nil}

	// ErrBucketNameRequired is returned when creating a bucket with a blank name.
	ErrBucketNameRequired = &Error{"bucket name required", nil}

	// ErrBucketNameTooLarge is returned when creating a bucket with a name
	// that is longer than MaxBucketNameSize.
	ErrBucketNameTooLarge = &Error{"bucket name too large", nil}

	// ErrBucketNotWritable is returned when changing data on a bucket
	// reference that was created from a read-only transaction.
	ErrBucketNotWritable = &Error{"bucket not writable", nil}

	// ErrKeyRequired is returned when inserting a zero-length key.
	ErrKeyRequired = &Error{"key required", nil}

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = &Error{"key too large", nil}

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	ErrValueTooLarge = &Error{"value too large", nil}

	// ErrSequenceOverflow is returned when the next sequence number will be
	// larger than the maximum integer size.
	ErrSequenceOverflow = &Error{"sequence overflow", nil}
)

// Error represents an error condition caused by Bolt.
type Error struct {
	message string
	cause   error
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e.cause != nil {
		return e.message + ": " + e.cause.Error()
	}
	return e.message
}

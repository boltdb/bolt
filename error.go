package bolt

var (
	// InvalidError is returned when a data file is not a Bolt-formatted database.
	InvalidError = &Error{"Invalid database", nil}

	// VersionMismatchError is returned when the data file was created with a 
	// different version of Bolt.
	VersionMismatchError = &Error{"version mismatch", nil}

	// DatabaseNotOpenError is returned when a DB instance is accessed before it
	// is opened or after it is closed.
	DatabaseNotOpenError = &Error{"database not open", nil}

	// DatabaseOpenError is returned when opening a database that is
	// already open.
	DatabaseOpenError = &Error{"database already open", nil}

	// BucketNotFoundError is returned when trying to access a bucket that has
	// not been created yet.
	BucketNotFoundError = &Error{"bucket not found", nil}

	// BucketExistsError is returned when creating a bucket that already exists.
	BucketExistsError = &Error{"bucket already exists", nil}

	// BucketNameRequiredError is returned when creating a bucket with a blank name.
	BucketNameRequiredError = &Error{"bucket name required", nil}

	// BucketNameTooLargeError is returned when creating a bucket with a name
	// that is longer than MaxBucketNameSize.
	BucketNameTooLargeError = &Error{"bucket name too large", nil}

	// KeyRequiredError is returned when inserting a zero-length key.
	KeyRequiredError = &Error{"key required", nil}

	// KeyTooLargeError is returned when inserting a key that is larger than MaxKeySize.
	KeyTooLargeError = &Error{"key too large", nil}

	// ValueTooLargeError is returned when inserting a value that is larger than MaxValueSize.
	ValueTooLargeError = &Error{"value too large", nil}
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

package bolt

var (
	InvalidError               = &Error{"Invalid database", nil}
	VersionMismatchError       = &Error{"version mismatch", nil}
	DatabaseNotOpenError       = &Error{"db is not open", nil}
	DatabaseAlreadyOpenedError = &Error{"db already open", nil}
	TransactionInProgressError = &Error{"writable transaction is already in progress", nil}
	InvalidTransactionError    = &Error{"txn is invalid", nil}
	BucketAlreadyExistsError   = &Error{"bucket already exists", nil}
)

type Error struct {
	message string
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return e.message + ": " + e.cause.Error()
	}
	return e.message
}

package bolt

var (
	KeyExistError        = &Error{"key/data pair already exists", nil}
	NotFoundError        = &Error{"no matching key/data pair found", nil}
	PageNotFoundError    = &Error{"requested page not found", nil}
	CorruptedError       = &Error{"located page was wrong type", nil}
	PanicError           = &Error{"update of meta page failed", nil}
	VersionMismatchError = &Error{"database environment version mismatch", nil}
	InvalidError         = &Error{"file is not a bolt file", nil}
	MapFullError         = &Error{"environment mapsize limit reached", nil}
	BucketFullError      = &Error{"environment maxdbs limit reached", nil}
	ReadersFullError     = &Error{"environment maxreaders limit reached", nil}
	TransactionFullError = &Error{"transaction has too many dirty pages - transaction too big", nil}
	CursorFullError      = &Error{"internal error - cursor stack limit reached", nil}
	PageFullError        = &Error{"internal error - page has no more space", nil}
	MapResizedError      = &Error{"database contents grew beyond environment mapsize", nil}
	IncompatibleError    = &Error{"operation and db incompatible, or db flags changed", nil}
	BadReaderSlotError   = &Error{"invalid reuse of reader locktable slot", nil}
	BadTransactionError  = &Error{"transaction cannot recover - it must be aborted", nil}
	BadValueSizeError    = &Error{"too big key/data or key is empty", nil}
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

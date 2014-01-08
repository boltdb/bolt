package bolt

var (
	KeyExistError        = &Error{"Key/data pair already exists"}
	NotFoundError        = &Error{"No matching key/data pair found"}
	PageNotFoundError    = &Error{"Requested page not found"}
	CorruptedError       = &Error{"Located page was wrong type"}
	PanicError           = &Error{"Update of meta page failed"}
	VersionMismatchError = &Error{"Database environment version mismatch"}
	InvalidError         = &Error{"File is not an MDB file"}
	MapFullError         = &Error{"Environment mapsize limit reached"}
	BucketFullError      = &Error{"Environment maxdbs limit reached"}
	ReadersFullError     = &Error{"Environment maxreaders limit reached"}
	TransactionFullError = &Error{"Transaction has too many dirty pages - transaction too big"}
	CursorFullError      = &Error{"Internal error - cursor stack limit reached"}
	PageFullError        = &Error{"Internal error - page has no more space"}
	MapResizedError      = &Error{"Database contents grew beyond environment mapsize"}
	IncompatibleError    = &Error{"Operation and DB incompatible, or DB flags changed"}
	BadReaderSlotError   = &Error{"Invalid reuse of reader locktable slot"}
	BadTransactionError  = &Error{"Transaction cannot recover - it must be aborted"}
	BadValueSizeError    = &Error{"Too big key/data, key is empty, or wrong DUPFIXED size"}
)

type Error struct {
	message string
}

func (e *Error) Error() {
	return e.message
}

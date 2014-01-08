package bolt

const (
	NoSync = iota
	NoMetaSync
	DupSort
	IntegerKey
	IntegerDupKey
)

type DB interface {
}

type db struct {
}

func NewDB() DB {
	return &db{}
}

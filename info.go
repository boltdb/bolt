package bolt

// info contains information about the database.
type info struct {
	MapSize           int
	LastPageID        int
	LastTransactionID int
	MaxReaders        int
	ReaderCount       int
}

package bolt

type Stat struct {
	PageSize          int
	Depth             int
	BranchPageCount   int
	LeafPageCount     int
	OverflowPageCount int
	EntryCount        int
}

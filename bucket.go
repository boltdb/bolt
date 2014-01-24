package bolt

type bucketid uint32

type Bucket struct {
	*bucket
	name string
}

type bucket struct {
	id       bucketid
	flags    uint32
	root     pgid
	branches pgid
	leafs    pgid
	entries  uint64
}

package bolt

type lnodes []lnode

// replace replaces the node at the given index with a new key/value size.
func (s lnodes) replace(key, value []byte, index int) lnodes {
	n := &s[index]
	n.pos = 0
	n.ksize = len(key)
	n.vsize = len(value)
	return s
}

// insert places a new node at the given index with a key/value size.
func (s lnodes) insert(key, value []byte, index int) lnodes {
	return append(s[0:index], lnode{ksize: len(key), vsize: len(value)}, s[index:len(s)])
}

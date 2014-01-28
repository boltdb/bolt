package bolt

type tnodes []tnode

type tnode struct {
	key   []byte
	value []byte
}

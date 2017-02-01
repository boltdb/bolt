package bolt

import (
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

// Ensure that a node can insert a key/value.
func TestNode_put(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{meta: &meta{pgid: 1}}}}
	n.put([]byte("baz"), []byte("baz"), []byte("2"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("0"), 0, 0)
	n.put([]byte("bar"), []byte("bar"), []byte("1"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("3"), 0, leafPageFlag)

	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "1" {
		t.Fatalf("exp=<bar,1>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "baz" || string(v) != "2" {
		t.Fatalf("exp=<baz,2>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "foo" || string(v) != "3" {
		t.Fatalf("exp=<foo,3>; got=<%s,%s>", k, v)
	}
	if n.inodes[2].flags != uint32(leafPageFlag) {
		t.Fatalf("not a leaf: %d", n.inodes[2].flags)
	}
}

// Ensure that a node can deserialize from a leaf page.
func TestNode_read_LeafPage(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = leafPageFlag
	page.count = 2

	// Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
	nodes := (*[3]leafPageElement)(unsafe.Pointer(&page.ptr))
	nodes[0] = leafPageElement{flags: 0, pos: 32, ksize: 3, vsize: 4}  // pos = sizeof(leafPageElement) * 2
	nodes[1] = leafPageElement{flags: 0, pos: 23, ksize: 10, vsize: 3} // pos = sizeof(leafPageElement) + 3 + 4

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&nodes[2]))
	copy(data[:], []byte("barfooz"))
	copy(data[7:], []byte("helloworldbye"))

	// Deserialize page into a leaf.
	n := &node{}
	n.bucket = &Bucket{
		tx: &Tx{
			db: &DB{},
		},
	}
	n.bucket.tx.db.Compress = false

	n.read(page)

	// Check that there are two inodes with correct data.
	if !n.isLeaf {
		t.Fatal("expected leaf")
	}
	if len(n.inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "fooz" {
		t.Fatalf("exp=<bar,fooz>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "helloworld" || string(v) != "bye" {
		t.Fatalf("exp=<helloworld,bye>; got=<%s,%s>", k, v)
	}
}

// Ensure that a node can serialize into a leaf page.
func TestNode_write_LeafPageCompressed(t *testing.T) {
	// Create a node.
	testlength := 102400
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{Compress: true}, meta: &meta{pgid: 1}}}}
	data1 := getRandomData(testlength)
	data2 := getRandomData(testlength)
	data3 := getRandomData(testlength)
	n.put([]byte("susy"), []byte("susy"), data1, 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), data2, 0, 0)
	n.put([]byte("john"), []byte("john"), data3, 0, 0)

	var buf = make([]byte, 4*testlength)
	p := (*page)(unsafe.Pointer(&buf[0]))
	n.write(p)

	// Read the page back in.
	n2 := &node{}
	n2.bucket = &Bucket{
		tx: &Tx{
			db: &DB{Compress: true},
		},
	}
	n2.read(p)

	// Check that the two pages are the same.
	if len(n2.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n2.inodes))
	}
	if k, v := n2.inodes[0].key, n2.inodes[0].value; string(k) != "john" || string(v) != string(data3) {
		// t.Fatalf("exp=<john,johnson>; got=<%s,%s>", k, v)
		t.Fatal("data corruption")
	}
	if k, v := n2.inodes[1].key, n2.inodes[1].value; string(k) != "ricki" || string(v) != string(data2) {
		// t.Fatalf("exp=<ricki,lake>; got=<%s,%s>", k, v)
		t.Fatal("data corruption")
	}
	if k, v := n2.inodes[2].key, n2.inodes[2].value; string(k) != "susy" || string(v) != string(data1) {
		// t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
		t.Fatal("data corruption")
	}
}

// Ensure that a node can serialize into a leaf page.
func TestNode_write_LeafPageUnCompressed(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson"), 0, 0)

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	n.write(p)

	// Read the page back in.
	n2 := &node{}
	n2.bucket = &Bucket{
		tx: &Tx{
			db: &DB{Compress: true},
		},
	}
	n2.bucket.tx.db.Compress = false
	n2.read(p)

	// Check that the two pages are the same.
	if len(n2.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n2.inodes))
	}
	if k, v := n2.inodes[0].key, n2.inodes[0].value; string(k) != "john" || string(v) != "johnson" {
		t.Fatalf("exp=<john,johnson>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[1].key, n2.inodes[1].value; string(k) != "ricki" || string(v) != "lake" {
		t.Fatalf("exp=<ricki,lake>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[2].key, n2.inodes[2].value; string(k) != "susy" || string(v) != "que" {
		t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
	}
}

// Ensure that a node can serialize into a leaf page.
func TestNode_write_LeafPage(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson"), 0, 0)

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	n.write(p)

	// Read the page back in.
	n2 := &node{}
	n2.bucket = &Bucket{
		tx: &Tx{
			db: &DB{},
		},
	}
	n2.bucket.tx.db.Compress = false
	n2.read(p)

	// Check that the two pages are the same.
	if len(n2.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n2.inodes))
	}
	if k, v := n2.inodes[0].key, n2.inodes[0].value; string(k) != "john" || string(v) != "johnson" {
		t.Fatalf("exp=<john,johnson>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[1].key, n2.inodes[1].value; string(k) != "ricki" || string(v) != "lake" {
		t.Fatalf("exp=<ricki,lake>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[2].key, n2.inodes[2].value; string(k) != "susy" || string(v) != "que" {
		t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
	}
}

// Ensure that a node can split into appropriate subgroups.
func TestNode_split(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	// Split between 2 & 3.
	n.split(100)

	var parent = n.parent
	if len(parent.children) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children))
	}
	if len(parent.children[0].inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children[0].inodes))
	}
	if len(parent.children[1].inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(parent.children[1].inodes))
	}
}

// Ensure that a page with the minimum number of inodes just returns a single node.
func TestNode_split_MinKeys(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)

	// Split.
	n.split(20)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}

// Ensure that a node that has keys that all fit on a page just returns one leaf.
func TestNode_split_SinglePage(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	// Split.
	n.split(4096)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}

func TestNode_CompressDecompressData(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake johnson lake johnson lake johnson lake johnson lake johnson lake johnson"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson lake johnson lake johnson lake johnson lake johnson lake johnson lake johnson lake"), 0, 0)

	// compress it
	presize := n.size()
	err := n.compress(presize)
	if err != nil {
		t.Fatalf("compression failed %v", err)
	}
	if !n.compressed {
		t.Fatalf("compression failed to complete")
	}
	postsize := n.size()
	t.Log("presize: ", presize, " postsize: ", postsize)
	// decompress it
	if n.decompress() != nil {
		t.Fatalf("Failed to decompress node")
	}
	if presize != n.size() {
		t.Fatalf("Compression failed to reproduce original size %v != %v", presize, n.size())
	}
	// Check that the two pages are the same.
	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "john" || string(v) != "johnson lake johnson lake johnson lake johnson lake johnson lake johnson lake johnson lake" {
		t.Fatalf("exp=<john,johnson lake>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "ricki" || string(v) != "lake johnson lake johnson lake johnson lake johnson lake johnson lake johnson" {
		t.Fatalf("exp=<ricki,lake johnson>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "susy" || string(v) != "que" {
		t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
	}
}
func TestNode_CompressDecompressDataTooBigForPage(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake johnson lake johnson lake johnson lake johnson lake johnson lake johnson"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson lake johnson lake johnson lake johnson lake johnson lake johnson lake johnson lake"), 0, 0)

	// compress it
	n.compress(1)

	if n.compressed {
		t.Fatalf("compression bigger than a page")
	}

}
func TestNode_CompressUncompressable(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson"), 0, 0)

	// compress it
	presize := n.size()
	err := n.compress(presize)
	if err != nil && err != ErrNotCompressed {
		t.Fatalf("compression failed %v", err)
	}
	if n.compressed {
		t.Fatalf("compression proceeeded anyway")
	}
	postsize := n.size()
	t.Log("presize: ", presize, " postsize: ", postsize)
	// decompress it
	err = n.decompress()
	if err != nil && err != ErrNotCompressed {
		t.Fatalf("Failed to decompress node")
	}
	if presize != n.size() {
		t.Fatalf("Compression failed to reproduce original size %v != %v", presize, n.size())
	}
	// Check that the two pages are the same.
	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "john" || string(v) != "johnson" {
		t.Fatalf("exp=<john,johnson>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "ricki" || string(v) != "lake" {
		t.Fatalf("exp=<ricki,lake>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "susy" || string(v) != "que" {
		t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
	}
}
func TestNode_CompressDecompressDataMultiValue(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	data1 := getRandomData(10240)
	data2 := getRandomData(102400)
	data3 := getRandomData(1024000)
	n.put([]byte("susy"), []byte("susy"), data1, 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), data2, 0, 0)
	n.put([]byte("john"), []byte("john"), data3, 0, 0)

	// compress it
	for _, item := range n.inodes {
		t.Logf("precompressed: %v:size %v", string(item.key), len(item.value))
	}
	presize := n.size()
	err := n.compress(presize)
	if err != nil && err != ErrNotCompressed {
		t.Fatalf("compression failed %v", err)
	}
	if !n.compressed {
		t.Fatalf("compression failed to complete")
	}
	postsize := n.size()
	for _, item := range n.inodes {
		t.Logf("postcompressed: %v:%v", string(item.key), len(item.value))
	}
	t.Log("presize: ", presize, " postsize: ", postsize)
	// decompress it
	err = n.decompress()
	if err != nil {
		t.Fatalf("Failed to decompress node %v", err)
	}
	for _, item := range n.inodes {
		t.Logf("postuncompressed: %v:size %v", string(item.key), len(item.value))
	}
	if presize != n.size() {
		t.Fatalf("Compression failed to reproduce original size %v != %v", presize, n.size())
	}
	// Check that the two pages are the same.
	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "john" || string(v) != string(data3) {
		t.Fatalf("exp=<john,%v>; got=<%s,%s>", data3, k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "ricki" || string(v) != string(data2) {
		t.Fatalf("exp=<ricki,%v>; got=<%s,%s>", data2, k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "susy" || string(v) != string(data1) {
		t.Fatalf("exp=<susy,%v>\n          got=<%s,%s>", data1, k, v)
	}
}

func getRandomData(size int) []byte {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz1234567890                    "
	data := make([]byte, size)
	data[0] = []byte("{")[0]
	for i := 1; i < size-1; i++ {
		data[i] = chars[rand.Intn(len(chars))]
	}
	data[size-1] = []byte("}")[0]
	return data
}

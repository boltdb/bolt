package bolt

import (
	"reflect"
	"testing"
)

// Ensure that the page type can be returned in human readable format.
func TestPage_typ(t *testing.T) {
	if typ := (&page{flags: branchPageFlag}).typ(); typ != "branch" {
		t.Fatalf("exp=branch; got=%v", typ)
	}
	if typ := (&page{flags: leafPageFlag}).typ(); typ != "leaf" {
		t.Fatalf("exp=leaf; got=%v", typ)
	}
	if typ := (&page{flags: metaPageFlag}).typ(); typ != "meta" {
		t.Fatalf("exp=meta; got=%v", typ)
	}
	if typ := (&page{flags: freelistPageFlag}).typ(); typ != "freelist" {
		t.Fatalf("exp=freelist; got=%v", typ)
	}
	if typ := (&page{flags: 20000}).typ(); typ != "unknown<4e20>" {
		t.Fatalf("exp=unknown<4e20>; got=%v", typ)
	}
}

// Ensure that the hexdump debugging function doesn't blow up.
func TestPage_dump(t *testing.T) {
	(&page{id: 256}).hexdump(16)
}

func TestPgids_Merge(t *testing.T) {
	a := pgids{4, 5, 6, 10, 11, 12, 13, 27}
	b := pgids{1, 3, 8, 9, 25, 30}
	c := a.Merge(b)
	if !reflect.DeepEqual(c, pgids{1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30}) {
		t.Errorf("mismatch: %v", c)
	}
	a = pgids{4, 5, 6, 10, 11, 12, 13, 27, 35, 36}
	b = pgids{8, 9, 25, 30}
	c = a.Merge(b)
	if !reflect.DeepEqual(c, pgids{4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30, 35, 36}) {
		t.Errorf("mismatch: %v", c)
	}
}

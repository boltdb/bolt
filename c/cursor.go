package c

/*

#define MAX_DEPTH	100
#define BRANCH_PAGE	1

// These types MUST have the same layout as their corresponding Go types

typedef unsigned long long	pageid;
typedef unsigned short		elemid;

typedef struct page {
	pgid			id;
	unsigned short	flags;
	elemid			count;
	unsigned long	overflow;
} page;

typedef struct branch_elem {
	unsigned long	pos;
	unsigned long	ksize;
	pgid			pgid;
} branch_elem;

typedef struct leaf_elem {
	unsigned long	flags;
	unsigned long	pos;
	unsigned long	ksize;
	unsigned long	vsize;
} leaf_elem;

// private types

typedef struct elem_ref {
	void	*element;
	page	*page;
	elemid	index;
} elem_ref;

// public types

typedef struct bolt_val {
    uint32_t size;
    void     *data;
} bolt_val;

typedef struct bolt_cursor {
	void			*data;
	pgid			root;
	size_t			pgsz;
	elemid[MAX_DEPTH]	stack;
	unsigned int	stackp;
} bolt_cursor;

// public functions

void bolt_cursor_init(bolt_cursor* c, void *data, size_t pgsz, pgid root) {
	c->data = data;
	c->pgid = pgid;
	c->pgsz = pgsz;
}

int bolt_cursor_first(bolt_cursor* c, bolt_val *key, bolt_val *value) {
	leaf_elem* leaf;
	elem_ref* ref;

	// reset stack to initial state
	c->stackp = 0;
	ref = &c->stack[c->stackp]
	ref->page = page(c, c->pgid);
	ref->index = 0;
	
	// get current leaf element
	return leaf_element(c, key, value);
}

int bolt_cursor_next(bolt_cursor* c, bolt_val *key, bolt_val *value) {
	elem_ref* ref= &c->stack[c->stackp];
	
	// increment element index
	ref->index++;
	// if we're past last element pop the stack and repeat
	while (ref->index >= ref->page->count ) {
		c->stackp--;
		ref = &c->stack[c->stackp];
		ref->index++;
	}
	// increment element pointer
	if(ref->page | BRANCH_PAGE)
		ref->element += sizeof(branch_elem);
	else
		ref->element += sizeof(leaf_elem);
		
	// get current leaf element
	return leaf_element(c, key, value);		
}

// int bolt_cursor_seek(bolt_cursor* c, bolt_val key, bolt_val *actual_key, bolt_val *value)

// private functions

page* page(bolt_cursor* c, pgid id) {
	return (page*)(c->data + (c->pgsz * id));
}

branch_elem* branch_page_element(page* p, elemid index) {
	return p + sizeof(page) + index * sizeof(branch_elem);
}

leaf_elem* leaf_page_element(page* p, elemid index) {
	return p + sizeof(page) + index * sizeof(leaf_elem);
}

// return current leaf element
// if stack points at a branch page descend down to the first elemenet
// of the first leaf page
int leaf_element(bolt_cursor* c, bolt_val *key, bolt_val *value) {
	elem_ref* ref = &c->stack[c->stackp];
	branch_elem* branch;
	for(ref->page->flags | BRANCH_PAGE) {
		branch = branch_page_element(ref->page,ref->index);
		c->stackp++;
		ref = &c->stack[c->stackp];
		ref->index = 0;
		ref->element = branch;
		ref->page = page(c, branch->pgid);
	};
	set_key_value(leaf_page_element(ref->page,ref->index), key, value);
	return 0
}

set_key_value(leaf_elem* leaf, bolt_val* key, bolt_val *value) {
	key.size = leaf->ksize;
	key.data = leaf + leaf->pos;
	value.size = leaf->vsize;
	value.data = key.data + key.size;	
}

*/
import "C"
import "github.com/boltdb/bolt"

func  NewCursor(b *bolt.Bucket) *C.bolt_cursor {
	data := (*C.void)(&b.tx.db.data[0])]
	pgsz := (C.size_t)(b.tx.db.pageSize)
	cursor := new(C.bolt_cursor)
	C.bolt_cursor_init(cursor, data, pgsz, (C.pgid)(b.root))
	return cursor
}

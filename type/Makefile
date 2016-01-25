all: gen

gen:
	TMPL_FILE_PATH=internal/cmd/gen-bolttype/bolttype.gen.go.tmpl GO_FILE_PATH=bolttype.gen.go go run internal/cmd/gen-bolttype/main.go

.PHONY: \
	all \
	gen

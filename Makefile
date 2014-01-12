PKG=./...
TEST=.
BENCH=.
COVERPROFILE=/tmp/c.out

bench: benchpreq
	go test -v -test.bench=$(BENCH) ./.bench

cover: fmt
	go test -coverprofile=$(COVERPROFILE) .
	go tool cover -html=$(COVERPROFILE)
	rm $(COVERPROFILE)

fmt:
	@go fmt ./...

test: fmt
	@go test -v -cover -test.run=$(TEST) $(PKG)

.PHONY: bench cover fmt test

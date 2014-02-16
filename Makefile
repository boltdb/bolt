TEST=.
BENCH=.
COVERPROFILE=/tmp/c.out

bench: benchpreq
	go test -v -test.bench=$(BENCH)

cover: fmt
	go test -coverprofile=$(COVERPROFILE) -test.run=$(TEST) .
	go tool cover -html=$(COVERPROFILE)
	rm $(COVERPROFILE)

fmt:
	@go fmt ./...

test: fmt
	@echo "=== TESTS ==="
	@go test -v -cover -test.run=$(TEST)
	@echo ""
	@echo ""
	@echo "=== RACE DETECTOR ==="
	@go test -v -race -test.run=Parallel

.PHONY: bench cover fmt test

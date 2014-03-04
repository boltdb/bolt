TEST=.
BENCH=.
COVERPROFILE=/tmp/c.out

bench: benchpreq
	go test -v -test.bench=$(BENCH)

# http://cloc.sourceforge.net/
cloc:
	@cloc --not-match-f='Makefile|_test.go' .

cover: fmt
	go test -coverprofile=$(COVERPROFILE) -test.run=$(TEST) .
	go tool cover -html=$(COVERPROFILE)
	rm $(COVERPROFILE)

cpuprofile: fmt
	@go test -c
	@./bolt.test -test.v -test.run="^X" -test.bench=$(BENCH) -test.cpuprofile cpu.prof

fmt:
	@go fmt ./...

test: fmt
	@echo "=== TESTS ==="
	@go test -v -cover -test.run=$(TEST)
	@echo ""
	@echo ""
	@echo "=== RACE DETECTOR ==="
	@go test -v -race -test.run=Parallel

.PHONY: bench cloc cover cpuprofile fmt memprofile test

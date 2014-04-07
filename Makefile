TEST=.
BENCH=.
COVERPROFILE=/tmp/c.out

bench: benchpreq
	go test -v -test.bench=$(BENCH)

# http://cloc.sourceforge.net/
cloc:
	@cloc --not-match-f='Makefile|_test.go' .

cover: fmt
	go test -coverprofile=$(COVERPROFILE) -test.run=$(TEST) $(COVERFLAG) .
	go tool cover -html=$(COVERPROFILE)
	rm $(COVERPROFILE)

cpuprofile: fmt
	@go test -c
	@./bolt.test -test.v -test.run="^X" -test.bench=$(BENCH) -test.cpuprofile cpu.prof

# go get github.com/kisielk/errcheck
errcheck:
	@echo "=== errcheck ==="
	@errcheck github.com/boltdb/bolt

fmt:
	@go fmt ./...

test: fmt errcheck
	@echo "=== TESTS ==="
	@go test -v -cover -test.run=$(TEST)
	@echo ""
	@echo ""
	@echo "=== RACE DETECTOR ==="
	@go test -v -race -test.run="TestSimulate_(100op|1000op|10000op)"

.PHONY: bench cloc cover cpuprofile fmt memprofile test

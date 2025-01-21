.PHONY: deps\:test-ci
deps\:test-ci:
	go install gotest.tools/gotestsum@latest

.PHONY: clean
clean:
	go clean -testcache

.PHONY: test
test:
	@$(MAKE) clean
	go test ./...

.PHONY: test\:ci
test\:ci:
# We do not make the deps here, the ci does that seperately to avoid compiling stuff
# multiple times etc.
	gotestsum --format testname ./...

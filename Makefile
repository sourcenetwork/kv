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

.PHONY: test\:all
test\:all:
# Environment variable changes do not invalidate the go test cache, so it is important
# for us to clean between each run.
	@$(MAKE) clean
	CORE_KV_MULTIPLIERS="memory" go test ./...
	@$(MAKE) clean
	CORE_KV_MULTIPLIERS="badger" go test ./...
	@$(MAKE) clean
	CORE_KV_MULTIPLIERS="namespace,memory" go test ./...
	@$(MAKE) clean
	CORE_KV_MULTIPLIERS="namespace,badger" go test ./...

.PHONY: test\:ci
test\:ci:
# We do not make the deps here, the ci does that seperately to avoid compiling stuff
# multiple times etc.
	gotestsum --format testname ./...

.PHONY: test\:scripts
test\:scripts:
	@$(MAKE) -C ./tools/scripts/ test

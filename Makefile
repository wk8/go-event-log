.DEFAULT_GOAL := all

.PHONY: all
all: test lint

# the TEST_FLAGS env var can be set to eg run only specific tests
TEST_COMMAND = go test -race -v -count=1 -cover $(TEST_FLAGS)

# the TEST_FLAGS env var can be set to eg run only specific tests
.PHONY: test
test:
	time $(TEST_COMMAND)

.PHONY: lint
lint:
	golangci-lint run

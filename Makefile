BUILD_DIR ?= build

.PHONY: all build test clean reset distclean re run-echo run-server help

all: build

build:
	@cmake -B $(BUILD_DIR)
	@cmake --build $(BUILD_DIR) -j

test: build
	@ctest --test-dir $(BUILD_DIR) --output-on-failure

run-echo: build
	@./$(BUILD_DIR)/echo_server

run-server: build
	@./$(BUILD_DIR)/high_level_server

clean:
	@rm -rf $(BUILD_DIR)

reset distclean:
	@./scripts/reset.sh

re:
	@./scripts/reset.sh --build

help:
	@echo "mini_reactor build shortcuts:"
	@echo "  make            - Build library, examples, and tests (CMake)"
	@echo "  make test       - Run all test suites via CTest"
	@echo "  make run-echo   - Build and start echo_server"
	@echo "  make run-server - Build and start high_level_server"
	@echo "  make clean      - Delete build directory"
	@echo "  make reset      - Clean build directory, caches, and orphaned files"
	@echo "  make re         - Reset workspace and rebuild fresh from scratch"


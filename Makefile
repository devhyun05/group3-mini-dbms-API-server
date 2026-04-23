CC ?= gcc
CFLAGS ?= -std=c11 -Wall -Wextra -Wno-unused-function -Wno-unused-variable -Wno-unused-but-set-variable -g -pthread -Isrc -Isrc/engine -Isrc/server
LDFLAGS ?= -pthread

TARGET = mini_dbms_api_server
ENGINE_SRC = \
	src/engine/lexer.c \
	src/engine/parser.c \
	src/engine/bptree.c \
	src/engine/executor.c \
	src/engine/db_engine.c
SERVER_SRC = \
	src/server/csapp.c \
	src/server/task_queue.c \
	src/server/http_server.c
APP_SRC = src/main.c
TEST_ENGINE = tests/test_engine
TEST_QUEUE = tests/test_queue
TEST_UNIT = tests/unit/test_unit
TEST_INTEGRATION = tests/integration/test_integration
TEST_FUNCTIONAL = tests/functional/test_functional.sh
TEST_EDGE = tests/edge/test_edge_cases.sh

.PHONY: all clean test test-unit test-integration test-functional test-edge

all: $(TARGET)

$(TARGET): $(APP_SRC) $(ENGINE_SRC) $(SERVER_SRC)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TEST_ENGINE): tests/test_engine.c $(ENGINE_SRC)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TEST_QUEUE): tests/test_queue.c src/server/task_queue.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TEST_UNIT): tests/unit/test_unit.c src/engine/lexer.c src/engine/parser.c src/server/task_queue.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TEST_INTEGRATION): tests/integration/test_integration.c $(ENGINE_SRC)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test-unit: $(TEST_UNIT)
	./$(TEST_UNIT)

test-integration: $(TEST_INTEGRATION)
	./$(TEST_INTEGRATION)

test-functional: $(TARGET)
	./$(TEST_FUNCTIONAL)

test-edge: $(TARGET)
	./$(TEST_EDGE)

test: test-unit test-integration test-functional test-edge

clean:
	rm -f $(TARGET) $(TEST_ENGINE) $(TEST_QUEUE) $(TEST_UNIT) $(TEST_INTEGRATION)

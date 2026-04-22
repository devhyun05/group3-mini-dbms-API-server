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

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(APP_SRC) $(ENGINE_SRC) $(SERVER_SRC)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TEST_ENGINE): tests/test_engine.c $(ENGINE_SRC)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(TEST_QUEUE): tests/test_queue.c src/server/task_queue.c
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test: $(TEST_ENGINE) $(TEST_QUEUE)
	./$(TEST_ENGINE)
	./$(TEST_QUEUE)
	./tests/test_api.sh

clean:
	rm -f $(TARGET) $(TEST_ENGINE) $(TEST_QUEUE)

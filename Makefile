CC ?= cc
APP_CFLAGS := -std=c11 -D_XOPEN_SOURCE=700 -Wall -Wextra -Werror -g -Isrc -Ivendor/sql-b-tree
VENDOR_CFLAGS := -std=c11 -D_XOPEN_SOURCE=700 -g -Ivendor/sql-b-tree
LDLIBS := -pthread
BUILD_DIR := build
TARGET := api_server

APP_OBJS := $(BUILD_DIR)/main.o
VENDOR_OBJS := \
	$(BUILD_DIR)/vendor_lexer.o \
	$(BUILD_DIR)/vendor_parser.o \
	$(BUILD_DIR)/vendor_bptree.o \
	$(BUILD_DIR)/vendor_executor.o

.PHONY: all test clean

all: $(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/main.o: src/main.c | $(BUILD_DIR)
	$(CC) $(APP_CFLAGS) -c $< -o $@

$(BUILD_DIR)/vendor_lexer.o: vendor/sql-b-tree/lexer.c | $(BUILD_DIR)
	$(CC) $(VENDOR_CFLAGS) -c $< -o $@

$(BUILD_DIR)/vendor_parser.o: vendor/sql-b-tree/parser.c | $(BUILD_DIR)
	$(CC) $(VENDOR_CFLAGS) -c $< -o $@

$(BUILD_DIR)/vendor_bptree.o: vendor/sql-b-tree/bptree.c | $(BUILD_DIR)
	$(CC) $(VENDOR_CFLAGS) -c $< -o $@

$(BUILD_DIR)/vendor_executor.o: vendor/sql-b-tree/executor.c | $(BUILD_DIR)
	$(CC) $(VENDOR_CFLAGS) -c $< -o $@

$(TARGET): $(APP_OBJS) $(VENDOR_OBJS)
	$(CC) $(APP_OBJS) $(VENDOR_OBJS) -o $@ $(LDLIBS)

test: $(TARGET)
	python3 tests/test_api.py

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

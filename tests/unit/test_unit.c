#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../../src/engine/lexer.h"
#include "../../src/engine/parser.h"
#include "../../src/server/task_queue.h"

static void fail(const char *message) {
    fprintf(stderr, "test_unit: %s\n", message);
    exit(1);
}

static void sleep_ms(long ms) {
    struct timespec ts;

    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

static void assert_token(Token token, SqlTokenType type, const char *text) {
    if (token.type != type) fail("unexpected token type");
    if (text && strcmp(token.text, text) != 0) fail("unexpected token text");
}

typedef struct {
    TaskQueue *queue;
    int result;
    volatile int done;
} PopThreadArgs;

static void *pop_thread_main(void *arg) {
    PopThreadArgs *args = (PopThreadArgs *)arg;

    args->result = task_queue_pop(args->queue);
    args->done = 1;
    return NULL;
}

static void test_lexer_keyword_and_string_tokens(void) {
    Lexer lexer;
    Token token;

    init_lexer(&lexer, "SELECT name FROM users WHERE email = 'alice@test.com' AND id BETWEEN 1 AND 3;");

    token = get_next_token(&lexer);
    assert_token(token, TOKEN_SELECT, "SELECT");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_IDENTIFIER, "name");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_FROM, "FROM");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_IDENTIFIER, "users");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_WHERE, "WHERE");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_IDENTIFIER, "email");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_EQ, "=");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_STRING, "alice@test.com");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_AND, "AND");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_IDENTIFIER, "id");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_BETWEEN, "BETWEEN");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_NUMBER, "1");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_AND, "AND");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_NUMBER, "3");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_SEMICOLON, ";");
    token = get_next_token(&lexer);
    assert_token(token, TOKEN_EOF, "");
}

static void test_parser_select_between_and_insert_values(void) {
    Statement stmt;

    if (!parse_statement("SELECT id FROM users WHERE id BETWEEN 1 AND 3", &stmt)) {
        fail("parse_statement should parse SELECT with BETWEEN");
    }
    if (stmt.type != STMT_SELECT) fail("parsed statement type should be SELECT");
    if (stmt.select_all) fail("SELECT column list should not be select_all");
    if (stmt.select_col_count != 1) fail("SELECT column list should contain one column");
    if (stmt.where_count != 1) fail("WHERE clause should contain one condition");
    if (stmt.where_conditions[0].type != WHERE_BETWEEN) fail("first WHERE condition should be BETWEEN");
    if (strcmp(stmt.where_conditions[0].col, "id") != 0) fail("first WHERE column should be id");
    if (strcmp(stmt.where_conditions[0].val, "1") != 0) fail("BETWEEN start should be 1");
    if (strcmp(stmt.where_conditions[0].end_val, "3") != 0) fail("BETWEEN end should be 3");

    if (!parse_statement("INSERT INTO users VALUES ('carol@test.com','Carol,QA')", &stmt)) {
        fail("parse_statement should parse INSERT with comma in quoted value");
    }
    if (stmt.type != STMT_INSERT) fail("parsed statement type should be INSERT");
    if (strcmp(stmt.table_name, "users") != 0) fail("INSERT target table should be users");
    if (strcmp(stmt.row_data, "'carol@test.com','Carol,QA'") != 0) fail("INSERT row_data should preserve quoted comma");
}

static void test_task_queue_fifo_and_full(void) {
    TaskQueue queue;

    if (!task_queue_init(&queue, 2)) fail("task_queue_init failed");
    if (!task_queue_push(&queue, 10)) fail("first push failed");
    if (!task_queue_push(&queue, 11)) fail("second push failed");
    if (task_queue_push(&queue, 12)) fail("push should fail when queue is full");
    if (task_queue_size(&queue) != 2) fail("queue size should be 2 after two pushes");
    if (task_queue_pop(&queue) != 10) fail("first pop should preserve FIFO order");
    if (task_queue_pop(&queue) != 11) fail("second pop should preserve FIFO order");
    task_queue_shutdown(&queue);
    task_queue_destroy(&queue);
}

static void test_task_queue_waits_then_wakes_on_signal(void) {
    TaskQueue queue;
    PopThreadArgs args;
    pthread_t thread;

    memset(&args, 0, sizeof(args));
    if (!task_queue_init(&queue, 4)) fail("task_queue_init failed");
    args.queue = &queue;

    if (pthread_create(&thread, NULL, pop_thread_main, &args) != 0) fail("failed to create pop thread");
    sleep_ms(100);
    if (args.done) fail("pop thread should still be waiting on an empty queue");

    if (!task_queue_push(&queue, 77)) fail("push should wake waiting pop thread");
    pthread_join(thread, NULL);
    if (!args.done) fail("pop thread should finish after push");
    if (args.result != 77) fail("woken pop thread should receive pushed fd");

    task_queue_shutdown(&queue);
    task_queue_destroy(&queue);
}

static void test_task_queue_shutdown_wakes_waiter(void) {
    TaskQueue queue;
    PopThreadArgs args;
    pthread_t thread;

    memset(&args, 0, sizeof(args));
    if (!task_queue_init(&queue, 4)) fail("task_queue_init failed");
    args.queue = &queue;

    if (pthread_create(&thread, NULL, pop_thread_main, &args) != 0) fail("failed to create pop thread");
    sleep_ms(100);
    if (args.done) fail("pop thread should be blocked before shutdown");

    task_queue_shutdown(&queue);
    pthread_join(thread, NULL);
    if (!args.done) fail("pop thread should wake after shutdown");
    if (args.result != -1) fail("shutdown pop should return -1");

    task_queue_destroy(&queue);
}

int main(void) {
    test_lexer_keyword_and_string_tokens();
    test_parser_select_between_and_insert_values();
    test_task_queue_fifo_and_full();
    test_task_queue_waits_then_wakes_on_signal();
    test_task_queue_shutdown_wakes_waiter();
    printf("test_unit: ok\n");
    return 0;
}

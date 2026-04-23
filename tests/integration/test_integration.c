#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../../src/engine/db_engine.h"

static void fail(const char *message) {
    fprintf(stderr, "test_integration: %s\n", message);
    exit(1);
}

static void sleep_ms(long ms) {
    struct timespec ts;

    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

static void remove_if_exists(const char *path) {
    if (unlink(path) != 0) {
        /* ignore cleanup failures */
    }
}

static void cleanup_dir(const char *dir) {
    char path[512];

    snprintf(path, sizeof(path), "%s/users.csv", dir);
    remove_if_exists(path);
    snprintf(path, sizeof(path), "%s/users.delta", dir);
    remove_if_exists(path);
    snprintf(path, sizeof(path), "%s/users.idx", dir);
    remove_if_exists(path);
    rmdir(dir);
}

static void write_fixture(const char *dir) {
    char path[512];
    FILE *file;

    mkdir(dir, 0755);
    snprintf(path, sizeof(path), "%s/users.csv", dir);
    file = fopen(path, "wb");
    if (!file) fail("failed to create fixture CSV");
    fprintf(file, "id(PK),email(UK),name(NN)\n");
    fprintf(file, "1,alice@test.com,Alice\n");
    fprintf(file, "2,bob@test.com,Bob\n");
    fclose(file);
}

typedef struct {
    DbEngine *engine;
    const char *sql;
    DbResult result;
    int ok;
    volatile int done;
} ExecuteThreadArgs;

static void *execute_thread_main(void *arg) {
    ExecuteThreadArgs *args = (ExecuteThreadArgs *)arg;

    memset(&args->result, 0, sizeof(args->result));
    args->ok = db_engine_execute(args->engine, args->sql, &args->result);
    args->done = 1;
    return NULL;
}

static void test_end_to_end_engine_chain_and_persistence(void) {
    char dir[] = "/tmp/mini_dbms_integration_XXXXXX";
    DbConfig config;
    DbEngine engine;
    DbResult result;

    if (!mkdtemp(dir)) fail("mkdtemp failed");
    write_fixture(dir);

    memset(&config, 0, sizeof(config));
    memset(&result, 0, sizeof(result));
    snprintf(config.data_dir, sizeof(config.data_dir), "%s", dir);
    config.quiet = 1;

    if (!db_engine_init(&engine, &config)) fail("db_engine_init failed");

    if (!db_engine_execute(&engine, "INSERT INTO users VALUES ('carol@test.com','Carol');", &result)) {
        fail("INSERT should succeed through db_engine_execute");
    }
    if (result.affected_rows != 1) fail("INSERT should affect one row");
    db_result_free(&result);
    memset(&result, 0, sizeof(result));

    db_engine_shutdown(&engine);

    if (!db_engine_init(&engine, &config)) fail("db_engine_init after restart failed");
    if (!db_engine_execute(&engine, "SELECT * FROM users WHERE email = 'carol@test.com';", &result)) {
        fail("SELECT after restart should succeed");
    }
    if (result.row_count != 1) fail("persisted row should still exist after restart");
    if (strcmp(result.access_path, "uk_index") != 0) fail("persisted SELECT should use uk_index");
    if (strcmp(result.rows[0].values[2], "Carol") != 0) fail("persisted row should keep inserted value");

    db_result_free(&result);
    db_engine_shutdown(&engine);
    cleanup_dir(dir);
}

static void test_execute_mutex_serializes_concurrent_engine_entry(void) {
    char dir[] = "/tmp/mini_dbms_lock_XXXXXX";
    DbConfig config;
    DbEngine engine;
    ExecuteThreadArgs args;
    pthread_t thread;

    if (!mkdtemp(dir)) fail("mkdtemp failed");
    write_fixture(dir);

    memset(&config, 0, sizeof(config));
    snprintf(config.data_dir, sizeof(config.data_dir), "%s", dir);
    config.quiet = 1;

    if (!db_engine_init(&engine, &config)) fail("db_engine_init failed");
    memset(&args, 0, sizeof(args));
    args.engine = &engine;
    args.sql = "SELECT * FROM users WHERE id = 1;";

    pthread_mutex_lock(&engine.execute_mutex);
    if (pthread_create(&thread, NULL, execute_thread_main, &args) != 0) {
        pthread_mutex_unlock(&engine.execute_mutex);
        fail("failed to create execute thread");
    }

    sleep_ms(100);
    if (args.done) {
        pthread_mutex_unlock(&engine.execute_mutex);
        fail("db_engine_execute should wait while execute_mutex is already held");
    }

    pthread_mutex_unlock(&engine.execute_mutex);
    pthread_join(thread, NULL);
    if (!args.done) fail("execute thread should finish after mutex unlock");
    if (!args.ok) fail("blocked db_engine_execute should eventually succeed");
    if (args.result.row_count != 1) fail("serialized SELECT should still return one row");
    if (strcmp(args.result.access_path, "pk_index") != 0) fail("serialized SELECT should use pk_index");

    db_result_free(&args.result);
    db_engine_shutdown(&engine);
    cleanup_dir(dir);
}

int main(void) {
    test_end_to_end_engine_chain_and_persistence();
    test_execute_mutex_serializes_concurrent_engine_entry();
    printf("test_integration: ok\n");
    return 0;
}

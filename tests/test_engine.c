#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/engine/db_engine.h"

static void fail(const char *message) {
    fprintf(stderr, "test_engine: %s\n", message);
    exit(1);
}

static void write_fixture(const char *dir) {
    char path[512];
    FILE *file;

    mkdir(dir, 0755);
    snprintf(path, sizeof(path), "%s/users.csv", dir);
    file = fopen(path, "wb");
    if (!file) fail("failed to open fixture file");
    fprintf(file, "id(PK),email(UK),name(NN)\n");
    fprintf(file, "1,alice@test.com,Alice\n");
    fprintf(file, "2,bob@test.com,Bob\n");
    fclose(file);
}

int main(void) {
    DbConfig config;
    DbEngine engine;
    DbResult result;
    const char *dir = "tests/tmp_engine_data";

    memset(&config, 0, sizeof(config));
    memset(&result, 0, sizeof(result));
    snprintf(config.data_dir, sizeof(config.data_dir), "%s", dir);
    config.quiet = 1;
    write_fixture(dir);

    if (!db_engine_init(&engine, &config)) fail("db_engine_init failed");

    if (!db_engine_execute(&engine, "SELECT * FROM users WHERE id = 1;", &result)) {
        fail(result.error_message);
    }
    if (result.row_count != 1) fail("select by pk should return one row");
    if (strcmp(result.access_path, "pk_index") != 0) fail("select by pk should use pk_index");
    if (strcmp(result.rows[0].values[2], "Alice") != 0) fail("unexpected select result");
    db_result_free(&result);
    memset(&result, 0, sizeof(result));

    if (!db_engine_execute(&engine, "INSERT INTO users VALUES ('carol@test.com','Carol');", &result)) {
        fail(result.error_message);
    }
    if (result.affected_rows != 1) fail("insert should affect one row");
    db_result_free(&result);
    memset(&result, 0, sizeof(result));

    if (!db_engine_execute(&engine, "SELECT * FROM users WHERE email = 'carol@test.com';", &result)) {
        fail(result.error_message);
    }
    if (result.row_count != 1) fail("select by uk should return one row");
    if (strcmp(result.access_path, "uk_index") != 0) fail("select by uk should use uk_index");
    if (strcmp(result.rows[0].values[2], "Carol") != 0) fail("unexpected inserted row");
    db_result_free(&result);
    memset(&result, 0, sizeof(result));

    if (!db_engine_execute(&engine, "UPDATE users SET name = 'Carolyn' WHERE email = 'carol@test.com';", &result)) {
        fail(result.error_message);
    }
    if (result.affected_rows != 1) fail("update should affect one row");
    if (strcmp(result.access_path, "uk_index") != 0) fail("update by uk should use uk_index");
    db_result_free(&result);
    memset(&result, 0, sizeof(result));

    if (!db_engine_execute(&engine, "DELETE FROM users WHERE id = 3;", &result)) {
        fail(result.error_message);
    }
    if (result.affected_rows != 1) fail("delete should affect one row");
    if (strcmp(result.access_path, "pk_index") != 0) fail("delete by pk should use pk_index");
    db_result_free(&result);
    memset(&result, 0, sizeof(result));

    if (db_engine_execute(&engine, "INSERT INTO users VALUES ('alice@test.com','Duplicate');", &result)) {
        fail("duplicate uk insert should fail");
    }
    if (result.error_code[0] == '\0') fail("duplicate uk insert should set error code");
    db_result_free(&result);

    db_engine_shutdown(&engine);
    unlink("tests/tmp_engine_data/users.csv");
    rmdir("tests/tmp_engine_data");
    return 0;
}

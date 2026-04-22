#include "db_engine.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db_engine_internal.h"
#include "executor.h"
#include "parser.h"

static _Thread_local DbEngine *g_bound_engine = NULL;
static _Thread_local DbResult *g_bound_result = NULL;

static char *db_strdup(const char *text) {
    size_t len;
    char *copy;

    if (!text) text = "";
    len = strlen(text) + 1;
    copy = (char *)malloc(len);
    if (!copy) return NULL;
    memcpy(copy, text, len);
    return copy;
}

static void db_normalize_sql(char *sql) {
    size_t len;

    if (!sql) return;
    len = strlen(sql);
    while (len > 0 && (sql[len - 1] == ' ' || sql[len - 1] == '\n' ||
                       sql[len - 1] == '\r' || sql[len - 1] == '\t')) {
        sql[--len] = '\0';
    }
    if (len > 0 && sql[len - 1] == ';') {
        sql[--len] = '\0';
    }
    while (len > 0 && (sql[len - 1] == ' ' || sql[len - 1] == '\n' ||
                       sql[len - 1] == '\r' || sql[len - 1] == '\t')) {
        sql[--len] = '\0';
    }
}

static void db_result_reset(DbResult *result) {
    if (!result) return;
    memset(result, 0, sizeof(*result));
}

static void db_result_write(char *buffer, size_t buffer_size, const char *fmt, va_list ap) {
    size_t length;

    if (!buffer || buffer_size == 0 || !fmt) return;
    vsnprintf(buffer, buffer_size, fmt, ap);
    buffer[buffer_size - 1] = '\0';
    length = strlen(buffer);
    while (length > 0 && (buffer[length - 1] == '\n' || buffer[length - 1] == '\r')) {
        buffer[length - 1] = '\0';
        length--;
    }
}

int db_engine_init(DbEngine *engine, const DbConfig *config) {
    if (!engine || !config) return 0;

    memset(engine, 0, sizeof(*engine));
    engine->config = *config;
    if (pthread_mutex_init(&engine->execute_mutex, NULL) != 0) {
        return 0;
    }
    return 1;
}

int db_engine_execute(DbEngine *engine, const char *sql, DbResult *out) {
    Statement stmt;
    TableCache *tc;
    char normalized_sql[MAX_SQL_LEN];
    int ok = 0;

    if (!engine || !sql || !out) return 0;

    db_result_free(out);
    db_result_reset(out);
    pthread_mutex_lock(&engine->execute_mutex);
    db_executor_bind_context(engine, out);
    set_executor_quiet(engine->config.quiet);

    strncpy(normalized_sql, sql, sizeof(normalized_sql) - 1);
    normalized_sql[sizeof(normalized_sql) - 1] = '\0';
    db_normalize_sql(normalized_sql);

    if (!parse_statement(normalized_sql, &stmt)) {
        db_result_set_error(out, "parse_error", "Failed to parse SQL statement.");
        goto done;
    }
    out->statement = stmt.type;

    tc = get_table(stmt.table_name);
    if (!tc) {
        if (out->error_code[0] == '\0') {
            db_result_set_error(out, "table_not_found", "Table '%s' could not be opened.", stmt.table_name);
        }
        goto done;
    }

    switch (stmt.type) {
        case STMT_INSERT:
            execute_insert(&stmt);
            break;
        case STMT_SELECT:
            execute_select(&stmt);
            break;
        case STMT_UPDATE:
            execute_update(&stmt);
            break;
        case STMT_DELETE:
            execute_delete(&stmt);
            break;
        default:
            db_result_set_error(out, "unsupported_statement", "Unsupported SQL statement.");
            break;
    }

done:
    out->ok = out->error_code[0] == '\0';
    ok = out->ok;
    db_executor_clear_context();
    pthread_mutex_unlock(&engine->execute_mutex);
    return ok;
}

void db_result_free(DbResult *result) {
    int row_index;

    if (!result) return;
    for (row_index = 0; row_index < result->column_count; row_index++) {
        free(result->columns[row_index]);
    }
    free(result->columns);
    result->columns = NULL;

    for (row_index = 0; row_index < result->row_count; row_index++) {
        int value_index;
        for (value_index = 0; value_index < result->rows[row_index].value_count; value_index++) {
            free(result->rows[row_index].values[value_index]);
        }
        free(result->rows[row_index].values);
    }
    free(result->rows);
    result->rows = NULL;
    result->column_count = 0;
    result->row_count = 0;
}

void db_engine_shutdown(DbEngine *engine) {
    if (!engine) return;
    db_executor_bind_context(engine, NULL);
    close_all_tables();
    db_executor_clear_context();
    pthread_mutex_destroy(&engine->execute_mutex);
}

void db_executor_bind_context(DbEngine *engine, DbResult *result) {
    g_bound_engine = engine;
    g_bound_result = result;
}

void db_executor_clear_context(void) {
    g_bound_engine = NULL;
    g_bound_result = NULL;
}

DbEngine *db_executor_current_engine(void) {
    return g_bound_engine;
}

DbResult *db_executor_current_result(void) {
    return g_bound_result;
}

void db_result_set_message(DbResult *result, const char *fmt, ...) {
    va_list ap;

    if (!result) return;
    va_start(ap, fmt);
    db_result_write(result->message, sizeof(result->message), fmt, ap);
    va_end(ap);
}

void db_result_set_error(DbResult *result, const char *code, const char *fmt, ...) {
    va_list ap;

    if (!result) return;
    if (code) {
        strncpy(result->error_code, code, sizeof(result->error_code) - 1);
        result->error_code[sizeof(result->error_code) - 1] = '\0';
    }
    va_start(ap, fmt);
    db_result_write(result->error_message, sizeof(result->error_message), fmt, ap);
    va_end(ap);
}

void db_result_set_access_path(DbResult *result, const char *access_path) {
    if (!result || !access_path) return;
    strncpy(result->access_path, access_path, sizeof(result->access_path) - 1);
    result->access_path[sizeof(result->access_path) - 1] = '\0';
}

void db_result_add_column(DbResult *result, const char *name) {
    char **new_columns;

    if (!result) return;
    new_columns = (char **)realloc(result->columns, (size_t)(result->column_count + 1) * sizeof(char *));
    if (!new_columns) return;
    result->columns = new_columns;
    result->columns[result->column_count] = db_strdup(name);
    if (!result->columns[result->column_count]) return;
    result->column_count++;
}

void db_result_add_row_values(DbResult *result, char **values, int value_count) {
    DbRow *new_rows;
    DbRow *row;
    int index;

    if (!result || !values || value_count < 0) return;
    new_rows = (DbRow *)realloc(result->rows, (size_t)(result->row_count + 1) * sizeof(DbRow));
    if (!new_rows) return;
    result->rows = new_rows;
    row = &result->rows[result->row_count];
    memset(row, 0, sizeof(*row));
    row->values = (char **)calloc((size_t)value_count, sizeof(char *));
    if (!row->values) return;
    row->value_count = value_count;
    for (index = 0; index < value_count; index++) {
        row->values[index] = db_strdup(values[index]);
    }
    result->row_count++;
}

void db_result_set_affected_rows(DbResult *result, int affected_rows) {
    if (!result) return;
    result->affected_rows = affected_rows;
}

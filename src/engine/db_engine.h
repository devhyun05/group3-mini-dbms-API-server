#ifndef DB_ENGINE_H
#define DB_ENGINE_H

#include "types.h"

typedef struct {
    char data_dir[512];
    int quiet;
} DbConfig;

typedef struct {
    int value_count;
    char **values;
} DbRow;

typedef struct {
    int ok;
    StatementType statement;
    int row_count;
    int affected_rows;
    int column_count;
    char **columns;
    DbRow *rows;
    char access_path[32];
    char message[512];
    char error_code[64];
    char error_message[512];
} DbResult;

typedef struct {
    DbConfig config;
    TableCache open_tables[MAX_TABLES];
    int open_table_count;
    pthread_mutex_t execute_mutex;
} DbEngine;

int db_engine_init(DbEngine *engine, const DbConfig *config);
int db_engine_execute(DbEngine *engine, const char *sql, DbResult *out);
void db_result_free(DbResult *out);
void db_engine_shutdown(DbEngine *engine);

#endif

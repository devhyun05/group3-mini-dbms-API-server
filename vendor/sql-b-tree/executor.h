#ifndef EXECUTOR_H
#define EXECUTOR_H

#include <stddef.h>

#include "types.h"

typedef enum {
    DB_RESULT_NONE = 0,
    DB_RESULT_SELECT,
    DB_RESULT_INSERT
} DbResultType;

typedef struct {
    DbResultType type;
    char **rows;
    size_t row_count;
    size_t row_capacity;
    int affected_rows;
    long inserted_id;
} DbResult;

void execute_insert(Statement *stmt);
void execute_select(Statement *stmt);
void execute_update(Statement *stmt);
void execute_delete(Statement *stmt);
void generate_jungle_dataset(int record_count, const char *filename);
void run_bplus_benchmark(int record_count);
void run_jungle_benchmark(int record_count);
void close_all_tables(void);
void set_executor_quiet(int quiet);
int db_execute_sql(const char *sql, DbResult *out, char *errbuf, size_t errbuf_size);
void db_free_result(DbResult *out);

#endif

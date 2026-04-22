#ifndef DB_ENGINE_INTERNAL_H
#define DB_ENGINE_INTERNAL_H

#include "db_engine.h"

void db_executor_bind_context(DbEngine *engine, DbResult *result);
void db_executor_clear_context(void);
DbEngine *db_executor_current_engine(void);
DbResult *db_executor_current_result(void);

void db_result_set_message(DbResult *result, const char *fmt, ...);
void db_result_set_error(DbResult *result, const char *code, const char *fmt, ...);
void db_result_set_access_path(DbResult *result, const char *access_path);
void db_result_add_column(DbResult *result, const char *name);
void db_result_add_row_values(DbResult *result, char **values, int value_count);
void db_result_set_affected_rows(DbResult *result, int affected_rows);

#endif

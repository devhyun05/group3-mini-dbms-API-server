#ifndef PARSER_H
#define PARSER_H

#include "types.h"

/* SQL 문자열을 파싱해 Statement로 변환합니다. */
/* true(1): 파싱 성공 / false(0): 파싱 실패 */
int parse_statement(const char *sql, Statement *stmt);

#endif


#ifndef LEXER_H
#define LEXER_H

#include "types.h"

/* lexer 상태를 초기화합니다. */
void init_lexer(Lexer *l, const char *sql);

/* 다음 토큰 하나를 읽어 반환합니다. */
Token get_next_token(Lexer *l);

#endif


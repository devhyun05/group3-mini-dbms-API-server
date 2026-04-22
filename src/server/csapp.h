#ifndef CSAPP_H
#define CSAPP_H

#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct sockaddr SA;

#define MAXLINE 8192
#define MAXBUF 8192
#define LISTENQ 1024
#define RIO_BUFSIZE 8192

typedef struct {
    int rio_fd;
    int rio_cnt;
    char *rio_bufptr;
    char rio_buf[RIO_BUFSIZE];
} rio_t;

void unix_error(const char *msg);
void posix_error(int code, const char *msg);
void gai_error(int code, const char *msg);

int Open_listenfd(char *port);
int Accept(int s, struct sockaddr *addr, socklen_t *addrlen);
void Close(int fd);
void Getnameinfo(const struct sockaddr *sa, socklen_t salen, char *host,
                 size_t hostlen, char *serv, size_t servlen, int flags);

void Rio_readinitb(rio_t *rp, int fd);
ssize_t Rio_readlineb(rio_t *rp, void *usrbuf, size_t maxlen);
ssize_t Rio_readnb(rio_t *rp, void *usrbuf, size_t n);
void Rio_writen(int fd, const void *usrbuf, size_t n);

#endif

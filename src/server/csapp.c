#include "csapp.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>

void unix_error(const char *msg) {
    fprintf(stderr, "%s: %s\n", msg, strerror(errno));
    exit(1);
}

void posix_error(int code, const char *msg) {
    fprintf(stderr, "%s: %s\n", msg, strerror(code));
    exit(1);
}

void gai_error(int code, const char *msg) {
    fprintf(stderr, "%s: %s\n", msg, gai_strerror(code));
    exit(1);
}

static int open_listenfd(char *port) {
    struct addrinfo hints;
    struct addrinfo *listp;
    struct addrinfo *p;
    int listenfd;
    int optval = 1;
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_ADDRCONFIG | AI_NUMERICSERV;
    if ((rc = getaddrinfo(NULL, port, &hints, &listp)) != 0) {
        fprintf(stderr, "getaddrinfo failed (port %s): %s\n", port, gai_strerror(rc));
        return -2;
    }

    for (p = listp; p; p = p->ai_next) {
        listenfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listenfd < 0) continue;
        setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval, sizeof(int));
        if (bind(listenfd, p->ai_addr, p->ai_addrlen) == 0) break;
        close(listenfd);
    }

    freeaddrinfo(listp);
    if (!p) return -1;
    if (listen(listenfd, LISTENQ) < 0) {
        close(listenfd);
        return -1;
    }
    return listenfd;
}

int Open_listenfd(char *port) {
    int rc = open_listenfd(port);
    if (rc < 0) unix_error("Open_listenfd error");
    return rc;
}

int Accept(int s, struct sockaddr *addr, socklen_t *addrlen) {
    int rc = accept(s, addr, addrlen);
    if (rc < 0 && errno != EINTR) unix_error("Accept error");
    return rc;
}

void Close(int fd) {
    if (close(fd) < 0) unix_error("Close error");
}

void Getnameinfo(const struct sockaddr *sa, socklen_t salen, char *host,
                 size_t hostlen, char *serv, size_t servlen, int flags) {
    int rc = getnameinfo(sa, salen, host, hostlen, serv, servlen, flags);
    if (rc != 0) gai_error(rc, "Getnameinfo error");
}

static ssize_t rio_read(rio_t *rp, char *usrbuf, size_t n) {
    int cnt;

    while (rp->rio_cnt <= 0) {
        rp->rio_cnt = (int)read(rp->rio_fd, rp->rio_buf, sizeof(rp->rio_buf));
        if (rp->rio_cnt < 0) {
            if (errno != EINTR) return -1;
        } else if (rp->rio_cnt == 0) {
            return 0;
        } else {
            rp->rio_bufptr = rp->rio_buf;
        }
    }

    cnt = (int)n;
    if (rp->rio_cnt < (int)n) cnt = rp->rio_cnt;
    memcpy(usrbuf, rp->rio_bufptr, (size_t)cnt);
    rp->rio_bufptr += cnt;
    rp->rio_cnt -= cnt;
    return cnt;
}

void Rio_readinitb(rio_t *rp, int fd) {
    rp->rio_fd = fd;
    rp->rio_cnt = 0;
    rp->rio_bufptr = rp->rio_buf;
}

ssize_t Rio_readlineb(rio_t *rp, void *usrbuf, size_t maxlen) {
    int n;
    ssize_t rc;
    char c;
    char *bufp = usrbuf;

    for (n = 1; n < (int)maxlen; n++) {
        rc = rio_read(rp, &c, 1);
        if (rc == 1) {
            *bufp++ = c;
            if (c == '\n') break;
        } else if (rc == 0) {
            if (n == 1) return 0;
            break;
        } else {
            return -1;
        }
    }
    *bufp = '\0';
    return n;
}

ssize_t Rio_readnb(rio_t *rp, void *usrbuf, size_t n) {
    size_t nleft = n;
    ssize_t nread;
    char *bufp = usrbuf;

    while (nleft > 0) {
        nread = rio_read(rp, bufp, nleft);
        if (nread < 0) return -1;
        if (nread == 0) break;
        nleft -= (size_t)nread;
        bufp += nread;
    }
    return (ssize_t)(n - nleft);
}

void Rio_writen(int fd, const void *usrbuf, size_t n) {
    size_t nleft = n;
    ssize_t nwritten;
    const char *bufp = usrbuf;

    while (nleft > 0) {
        nwritten = write(fd, bufp, nleft);
        if (nwritten <= 0) {
            if (errno == EINTR) nwritten = 0;
            else unix_error("Rio_writen error");
        }
        nleft -= (size_t)nwritten;
        bufp += nwritten;
    }
}

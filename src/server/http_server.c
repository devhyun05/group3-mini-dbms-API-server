#include "http_server.h"

#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/syslimits.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "../engine/db_engine.h"
#include "csapp.h"
#include "task_queue.h"

#define MAX_BODY_BYTES (64 * 1024)
#define FD_TIMESTAMP_CAP 65536

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} StringBuilder;

typedef struct {
    DbEngine engine;
    TaskQueue queue;
    pthread_t *workers;
    int worker_count;
    int listenfd;
    volatile sig_atomic_t shutting_down;
    long long enqueue_times[FD_TIMESTAMP_CAP];
} ApiServer;

static ApiServer *g_server = NULL;

static long long now_ns(void) {
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static double ns_to_ms(long long duration_ns) {
    return (double)duration_ns / 1000000.0;
}

static void handle_signal(int signo) {
    (void)signo;
    if (!g_server) return;
    g_server->shutting_down = 1;
    if (g_server->listenfd >= 0) close(g_server->listenfd);
}

static int sb_reserve(StringBuilder *sb, size_t extra) {
    size_t needed;
    char *new_data;

    if (!sb) return 0;
    needed = sb->length + extra + 1;
    if (needed <= sb->capacity) return 1;
    while (sb->capacity < needed) {
        sb->capacity = sb->capacity == 0 ? 512 : sb->capacity * 2;
    }
    new_data = (char *)realloc(sb->data, sb->capacity);
    if (!new_data) return 0;
    sb->data = new_data;
    return 1;
}

static int sb_append(StringBuilder *sb, const char *text) {
    size_t len;

    if (!sb || !text) return 0;
    len = strlen(text);
    if (!sb_reserve(sb, len)) return 0;
    memcpy(sb->data + sb->length, text, len);
    sb->length += len;
    sb->data[sb->length] = '\0';
    return 1;
}

static int sb_appendf(StringBuilder *sb, const char *fmt, ...) {
    va_list ap;
    va_list copy;
    int needed;

    if (!sb || !fmt) return 0;
    va_start(ap, fmt);
    va_copy(copy, ap);
    needed = vsnprintf(NULL, 0, fmt, copy);
    va_end(copy);
    if (needed < 0) {
        va_end(ap);
        return 0;
    }
    if (!sb_reserve(sb, (size_t)needed)) {
        va_end(ap);
        return 0;
    }
    vsnprintf(sb->data + sb->length, sb->capacity - sb->length, fmt, ap);
    va_end(ap);
    sb->length += (size_t)needed;
    return 1;
}

static int sb_append_json_string(StringBuilder *sb, const char *text) {
    const unsigned char *cur = (const unsigned char *)(text ? text : "");

    if (!sb_append(sb, "\"")) return 0;
    while (*cur) {
        switch (*cur) {
            case '\\':
                if (!sb_append(sb, "\\\\")) return 0;
                break;
            case '"':
                if (!sb_append(sb, "\\\"")) return 0;
                break;
            case '\n':
                if (!sb_append(sb, "\\n")) return 0;
                break;
            case '\r':
                if (!sb_append(sb, "\\r")) return 0;
                break;
            case '\t':
                if (!sb_append(sb, "\\t")) return 0;
                break;
            default:
                if (*cur < 0x20) {
                    if (!sb_appendf(sb, "\\u%04x", *cur)) return 0;
                } else {
                    char ch[2] = {(char)*cur, '\0'};
                    if (!sb_append(sb, ch)) return 0;
                }
                break;
        }
        cur++;
    }
    return sb_append(sb, "\"");
}

static void sb_free(StringBuilder *sb) {
    if (!sb) return;
    free(sb->data);
    sb->data = NULL;
    sb->length = 0;
    sb->capacity = 0;
}

static const char *statement_name(StatementType type) {
    switch (type) {
        case STMT_INSERT:
            return "INSERT";
        case STMT_SELECT:
            return "SELECT";
        case STMT_UPDATE:
            return "UPDATE";
        case STMT_DELETE:
            return "DELETE";
        default:
            return "UNKNOWN";
    }
}

static int extract_sql_from_json(const char *body, char *sql, size_t sql_size) {
    const char *cursor = body;

    if (!body || !sql || sql_size == 0) return 0;
    sql[0] = '\0';

    while ((cursor = strstr(cursor, "\"sql\"")) != NULL) {
        const char *colon;
        const char *quote;
        size_t out = 0;

        colon = cursor + 5;
        while (*colon == ' ' || *colon == '\n' || *colon == '\r' || *colon == '\t') colon++;
        if (*colon != ':') {
            cursor += 5;
            continue;
        }
        colon++;
        while (*colon == ' ' || *colon == '\n' || *colon == '\r' || *colon == '\t') colon++;
        if (*colon != '"') return 0;
        quote = colon + 1;
        while (*quote) {
            if (*quote == '\\') {
                quote++;
                if (!*quote) return 0;
                if (out + 1 >= sql_size) return 0;
                switch (*quote) {
                    case '"':
                    case '\\':
                    case '/':
                        sql[out++] = *quote;
                        break;
                    case 'n':
                        sql[out++] = '\n';
                        break;
                    case 'r':
                        sql[out++] = '\r';
                        break;
                    case 't':
                        sql[out++] = '\t';
                        break;
                    default:
                        return 0;
                }
            } else if (*quote == '"') {
                sql[out] = '\0';
                return 1;
            } else {
                if (out + 1 >= sql_size) return 0;
                sql[out++] = *quote;
            }
            quote++;
        }
        return 0;
    }

    return 0;
}

static void send_json_response(int fd, int status_code, const char *status_text, const char *json_body) {
    char header[MAXBUF];
    size_t body_len = json_body ? strlen(json_body) : 0;

    snprintf(header, sizeof(header),
             "HTTP/1.0 %d %s\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %zu\r\n"
             "Connection: close\r\n"
             "\r\n",
             status_code, status_text, body_len);
    Rio_writen(fd, header, strlen(header));
    if (body_len > 0) Rio_writen(fd, json_body, body_len);
}

static void send_error_response(int fd, int status_code, const char *status_text,
                                const char *code, const char *message,
                                double queue_wait_ms, double execute_ms) {
    StringBuilder sb = {0};

    sb_append(&sb, "{\"ok\":false,\"error\":{\"code\":");
    sb_append_json_string(&sb, code);
    sb_append(&sb, ",\"message\":");
    sb_append_json_string(&sb, message);
    sb_appendf(&sb, "},\"queue_wait_ms\":%.3f,\"execute_ms\":%.3f}", queue_wait_ms, execute_ms);
    send_json_response(fd, status_code, status_text, sb.data ? sb.data : "{\"ok\":false}");
    sb_free(&sb);
}

static void send_success_response(int fd, const DbResult *result,
                                  double queue_wait_ms, double execute_ms) {
    StringBuilder sb = {0};
    int row_index;
    int value_index;

    sb_append(&sb, "{\"ok\":true,\"statement\":");
    sb_append_json_string(&sb, statement_name(result->statement));
    sb_appendf(&sb, ",\"row_count\":%d,\"affected_rows\":%d", result->row_count, result->affected_rows);
    sb_append(&sb, ",\"columns\":[");
    for (row_index = 0; row_index < result->column_count; row_index++) {
        if (row_index > 0) sb_append(&sb, ",");
        sb_append_json_string(&sb, result->columns[row_index]);
    }
    sb_append(&sb, "],\"rows\":[");
    for (row_index = 0; row_index < result->row_count; row_index++) {
        if (row_index > 0) sb_append(&sb, ",");
        sb_append(&sb, "[");
        for (value_index = 0; value_index < result->rows[row_index].value_count; value_index++) {
            if (value_index > 0) sb_append(&sb, ",");
            sb_append_json_string(&sb, result->rows[row_index].values[value_index]);
        }
        sb_append(&sb, "]");
    }
    sb_append(&sb, "],\"access_path\":");
    sb_append_json_string(&sb, result->access_path[0] ? result->access_path : "");
    sb_append(&sb, ",\"message\":");
    sb_append_json_string(&sb, result->message[0] ? result->message : "ok");
    sb_appendf(&sb, ",\"queue_wait_ms\":%.3f,\"execute_ms\":%.3f}", queue_wait_ms, execute_ms);
    send_json_response(fd, 200, "OK", sb.data ? sb.data : "{\"ok\":true}");
    sb_free(&sb);
}

static int read_request_headers(rio_t *rio, long *content_length) {
    char line[MAXLINE];

    *content_length = 0;
    while (1) {
        ssize_t bytes = Rio_readlineb(rio, line, sizeof(line));
        if (bytes <= 0) return 0;
        if (strcmp(line, "\r\n") == 0 || strcmp(line, "\n") == 0) return 1;
        if (strncasecmp(line, "Content-Length:", 15) == 0) {
            *content_length = strtol(line + 15, NULL, 10);
        }
    }
}

static double server_take_queue_wait_ms(ApiServer *server, int fd) {
    long long enqueued_at;

    if (!server || fd < 0 || fd >= FD_TIMESTAMP_CAP) return 0.0;
    enqueued_at = server->enqueue_times[fd];
    server->enqueue_times[fd] = 0;
    if (enqueued_at == 0) return 0.0;
    return ns_to_ms(now_ns() - enqueued_at);
}

static void respond_queue_full(int fd) {
    send_error_response(fd, 503, "Service Unavailable",
                        "queue_full",
                        "Request queue is full. Try again later.",
                        0.0, 0.0);
}

static void doit(ApiServer *server, int fd, double queue_wait_ms) {
    DbResult result;
    rio_t rio;
    char request_line[MAXLINE];
    char method[32];
    char uri[256];
    char version[32];
    char *body = NULL;
    char sql[MAX_SQL_LEN];
    long content_length = 0;
    long long started_at = now_ns();
    double execute_ms = 0.0;
    int status_ok = 0;

    memset(&result, 0, sizeof(result));
    Rio_readinitb(&rio, fd);

    if (Rio_readlineb(&rio, request_line, sizeof(request_line)) <= 0) {
        return;
    }
    if (sscanf(request_line, "%31s %255s %31s", method, uri, version) != 3) {
        send_error_response(fd, 400, "Bad Request", "bad_request", "Malformed request line.", queue_wait_ms, 0.0);
        return;
    }
    if (!read_request_headers(&rio, &content_length)) {
        send_error_response(fd, 400, "Bad Request", "bad_headers", "Failed to read HTTP headers.", queue_wait_ms, 0.0);
        return;
    }
    if (strcmp(uri, "/query") != 0) {
        send_error_response(fd, 404, "Not Found", "not_found", "Only POST /query is supported.", queue_wait_ms, 0.0);
        return;
    }
    if (strcmp(method, "POST") != 0) {
        send_error_response(fd, 405, "Method Not Allowed", "method_not_allowed", "Use POST /query.", queue_wait_ms, 0.0);
        return;
    }
    if (content_length <= 0 || content_length > MAX_BODY_BYTES) {
        send_error_response(fd, 400, "Bad Request", "invalid_length", "Content-Length must be between 1 and 65536 bytes.", queue_wait_ms, 0.0);
        return;
    }

    body = (char *)calloc((size_t)content_length + 1, 1);
    if (!body) {
        send_error_response(fd, 500, "Internal Server Error", "oom", "Failed to allocate request buffer.", queue_wait_ms, 0.0);
        return;
    }
    if (Rio_readnb(&rio, body, (size_t)content_length) != content_length) {
        send_error_response(fd, 400, "Bad Request", "short_body", "Request body was shorter than Content-Length.", queue_wait_ms, 0.0);
        free(body);
        return;
    }
    body[content_length] = '\0';
    if (!extract_sql_from_json(body, sql, sizeof(sql))) {
        send_error_response(fd, 400, "Bad Request", "invalid_json", "Body must be JSON with a string field named 'sql'.", queue_wait_ms, 0.0);
        free(body);
        return;
    }

    status_ok = db_engine_execute(&server->engine, sql, &result);
    execute_ms = ns_to_ms(now_ns() - started_at);
    if (status_ok) {
        send_success_response(fd, &result, queue_wait_ms, execute_ms);
    } else {
        send_error_response(fd, 400, "Bad Request",
                            result.error_code[0] ? result.error_code : "query_error",
                            result.error_message[0] ? result.error_message : "Query execution failed.",
                            queue_wait_ms, execute_ms);
    }

    db_result_free(&result);
    free(body);
}

static void *worker_main(void *arg) {
    ApiServer *server = (ApiServer *)arg;

    while (1) {
        int fd = task_queue_pop(&server->queue);
        double queue_wait_ms;

        if (fd < 0) break;
        queue_wait_ms = server_take_queue_wait_ms(server, fd);
        doit(server, fd, queue_wait_ms);
        Close(fd);
    }
    return NULL;
}

static int start_workers(ApiServer *server) {
    int index;

    server->workers = (pthread_t *)calloc((size_t)server->worker_count, sizeof(pthread_t));
    if (!server->workers) return 0;
    for (index = 0; index < server->worker_count; index++) {
        if (pthread_create(&server->workers[index], NULL, worker_main, server) != 0) return 0;
    }
    return 1;
}

static void stop_workers(ApiServer *server) {
    int index;

    task_queue_shutdown(&server->queue);
    for (index = 0; index < server->worker_count; index++) {
        pthread_join(server->workers[index], NULL);
    }
    free(server->workers);
    server->workers = NULL;
}

int http_server_run(const char *port, const char *data_dir) {
    ApiServer server;
    DbConfig config;
    struct sockaddr_storage clientaddr;
    socklen_t clientlen;
    long cpu_count;

    memset(&server, 0, sizeof(server));
    memset(&config, 0, sizeof(config));
    strncpy(config.data_dir, data_dir ? data_dir : "data", sizeof(config.data_dir) - 1);
    config.quiet = 1;

    mkdir(config.data_dir, 0755);

    cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
    server.worker_count = cpu_count > 0 ? (int)cpu_count : 4;
    server.listenfd = -1;

    if (!db_engine_init(&server.engine, &config)) {
        fprintf(stderr, "failed to initialize db engine\n");
        return 1;
    }
    if (!task_queue_init(&server.queue, server.worker_count * 8 > 32 ? server.worker_count * 8 : 32)) {
        fprintf(stderr, "failed to initialize task queue\n");
        db_engine_shutdown(&server.engine);
        return 1;
    }
    if (!start_workers(&server)) {
        fprintf(stderr, "failed to start worker threads\n");
        task_queue_destroy(&server.queue);
        db_engine_shutdown(&server.engine);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    g_server = &server;
    server.listenfd = Open_listenfd((char *)port);

    while (!server.shutting_down) {
        int connfd;

        clientlen = sizeof(clientaddr);
        connfd = Accept(server.listenfd, (SA *)&clientaddr, &clientlen);
        if (connfd < 0) {
            if (server.shutting_down || errno == EINTR) continue;
            break;
        }
        if (connfd >= 0 && connfd < FD_TIMESTAMP_CAP) {
            server.enqueue_times[connfd] = now_ns();
        }
        if (!task_queue_push(&server.queue, connfd)) {
            if (connfd >= 0 && connfd < FD_TIMESTAMP_CAP) server.enqueue_times[connfd] = 0;
            respond_queue_full(connfd);
            Close(connfd);
        }
    }

    stop_workers(&server);
    task_queue_destroy(&server.queue);
    db_engine_shutdown(&server.engine);
    g_server = NULL;
    return 0;
}

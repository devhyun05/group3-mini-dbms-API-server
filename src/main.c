#define _XOPEN_SOURCE 700

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "executor.h"
#include "parser.h"

#ifndef PATH_MAX
#define PATH_MAX 1024
#endif

#define DEFAULT_PORT 8080
#define DEFAULT_WORKERS 4
#define DEFAULT_QUEUE_CAPACITY 64
#define MAX_HTTP_REQUEST_SIZE 16384
#define MAX_HTTP_BODY_SIZE 8192

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} StrBuf;

typedef struct {
    int *items;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
} JobQueue;

typedef struct {
    int port;
    int worker_count;
    int queue_capacity;
    char data_dir[PATH_MAX];
} AppConfig;

typedef struct {
    char method[8];
    char path[64];
    char *body;
    size_t body_length;
} HttpRequest;

static JobQueue g_job_queue;
static pthread_rwlock_t g_db_lock = PTHREAD_RWLOCK_INITIALIZER;

static char *dup_string(const char *src) {
    size_t len;
    char *copy;

    if (src == NULL) {
        return NULL;
    }

    len = strlen(src) + 1U;
    copy = (char *)malloc(len);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, src, len);
    return copy;
}

static void sb_init(StrBuf *sb) {
    memset(sb, 0, sizeof(*sb));
}

static void sb_free(StrBuf *sb) {
    if (sb == NULL) {
        return;
    }
    free(sb->data);
    memset(sb, 0, sizeof(*sb));
}

static int sb_reserve(StrBuf *sb, size_t extra) {
    char *grown;
    size_t required;
    size_t next_capacity;

    if (sb == NULL) {
        return 0;
    }

    required = sb->length + extra + 1U;
    if (required <= sb->capacity) {
        return 1;
    }

    next_capacity = sb->capacity == 0U ? 256U : sb->capacity;
    while (next_capacity < required) {
        next_capacity *= 2U;
    }

    grown = (char *)realloc(sb->data, next_capacity);
    if (grown == NULL) {
        return 0;
    }

    sb->data = grown;
    sb->capacity = next_capacity;
    return 1;
}

static int sb_append_len(StrBuf *sb, const char *text, size_t len) {
    if (text == NULL) {
        return 1;
    }
    if (!sb_reserve(sb, len)) {
        return 0;
    }
    memcpy(sb->data + sb->length, text, len);
    sb->length += len;
    sb->data[sb->length] = '\0';
    return 1;
}

static int sb_append(StrBuf *sb, const char *text) {
    return sb_append_len(sb, text, text == NULL ? 0U : strlen(text));
}

static int sb_appendf(StrBuf *sb, const char *fmt, ...) {
    va_list args;
    va_list copy;
    int written;

    va_start(args, fmt);
    va_copy(copy, args);
    written = vsnprintf(NULL, 0, fmt, copy);
    va_end(copy);
    if (written < 0) {
        va_end(args);
        return 0;
    }
    if (!sb_reserve(sb, (size_t)written)) {
        va_end(args);
        return 0;
    }
    vsnprintf(sb->data + sb->length, sb->capacity - sb->length, fmt, args);
    va_end(args);
    sb->length += (size_t)written;
    return 1;
}

static int sb_append_json_string(StrBuf *sb, const char *text) {
    const unsigned char *cursor;

    if (!sb_append(sb, "\"")) {
        return 0;
    }

    cursor = (const unsigned char *)(text == NULL ? "" : text);
    while (*cursor != '\0') {
        switch (*cursor) {
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
                if (*cursor < 0x20U) {
                    if (!sb_appendf(sb, "\\u%04x", (unsigned int)*cursor)) return 0;
                } else if (!sb_append_len(sb, (const char *)cursor, 1U)) {
                    return 0;
                }
                break;
        }
        cursor++;
    }

    return sb_append(sb, "\"");
}

static void free_http_request(HttpRequest *request) {
    if (request == NULL) {
        return;
    }
    free(request->body);
    memset(request, 0, sizeof(*request));
}

static const char *skip_ws(const char *cursor) {
    while (cursor != NULL && *cursor != '\0' && isspace((unsigned char)*cursor)) {
        cursor++;
    }
    return cursor;
}

static int parse_json_string(const char *cursor, char **out_text, const char **out_next,
                             char *errbuf, size_t errbuf_size) {
    StrBuf sb;

    if (cursor == NULL || out_text == NULL || out_next == NULL) {
        snprintf(errbuf, errbuf_size, "invalid JSON parser arguments");
        return 0;
    }
    if (*cursor != '"') {
        snprintf(errbuf, errbuf_size, "JSON string must start with a double quote");
        return 0;
    }

    sb_init(&sb);
    cursor++;
    while (*cursor != '\0') {
        if (*cursor == '"') {
            *out_text = sb.data == NULL ? dup_string("") : sb.data;
            *out_next = cursor + 1;
            return 1;
        }
        if (*cursor == '\\') {
            cursor++;
            if (*cursor == '\0') {
                sb_free(&sb);
                snprintf(errbuf, errbuf_size, "unterminated JSON escape");
                return 0;
            }
            switch (*cursor) {
                case '"':
                    if (!sb_append_len(&sb, "\"", 1U)) goto oom;
                    break;
                case '\\':
                    if (!sb_append_len(&sb, "\\", 1U)) goto oom;
                    break;
                case '/':
                    if (!sb_append_len(&sb, "/", 1U)) goto oom;
                    break;
                case 'b':
                    if (!sb_append_len(&sb, "\b", 1U)) goto oom;
                    break;
                case 'f':
                    if (!sb_append_len(&sb, "\f", 1U)) goto oom;
                    break;
                case 'n':
                    if (!sb_append_len(&sb, "\n", 1U)) goto oom;
                    break;
                case 'r':
                    if (!sb_append_len(&sb, "\r", 1U)) goto oom;
                    break;
                case 't':
                    if (!sb_append_len(&sb, "\t", 1U)) goto oom;
                    break;
                default:
                    sb_free(&sb);
                    snprintf(errbuf, errbuf_size, "unsupported JSON escape sequence");
                    return 0;
            }
        } else {
            if (!sb_append_len(&sb, cursor, 1U)) {
                goto oom;
            }
        }
        cursor++;
    }

    sb_free(&sb);
    snprintf(errbuf, errbuf_size, "unterminated JSON string");
    return 0;

oom:
    sb_free(&sb);
    snprintf(errbuf, errbuf_size, "out of memory while parsing JSON");
    return 0;
}

static int extract_sql_from_json(const char *body, char **out_sql, char *errbuf, size_t errbuf_size) {
    const char *cursor;
    char *key = NULL;
    char *value = NULL;

    *out_sql = NULL;
    cursor = skip_ws(body);
    if (cursor == NULL || *cursor != '{') {
        snprintf(errbuf, errbuf_size, "JSON body must be an object");
        return 0;
    }

    cursor = skip_ws(cursor + 1);
    if (!parse_json_string(cursor, &key, &cursor, errbuf, errbuf_size)) {
        return 0;
    }
    if (strcmp(key, "sql") != 0) {
        free(key);
        snprintf(errbuf, errbuf_size, "JSON body must contain only the \"sql\" field");
        return 0;
    }
    free(key);

    cursor = skip_ws(cursor);
    if (*cursor != ':') {
        snprintf(errbuf, errbuf_size, "missing colon after \"sql\"");
        return 0;
    }
    cursor = skip_ws(cursor + 1);
    if (!parse_json_string(cursor, &value, &cursor, errbuf, errbuf_size)) {
        return 0;
    }

    cursor = skip_ws(cursor);
    if (*cursor != '}') {
        free(value);
        snprintf(errbuf, errbuf_size, "JSON body must end after the \"sql\" field");
        return 0;
    }
    cursor = skip_ws(cursor + 1);
    if (*cursor != '\0') {
        free(value);
        snprintf(errbuf, errbuf_size, "unexpected trailing JSON content");
        return 0;
    }

    *out_sql = value;
    return 1;
}

static int send_all(int fd, const char *buffer, size_t length) {
    size_t sent = 0U;

    while (sent < length) {
        ssize_t written = send(fd, buffer + sent, length - sent, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return 0;
        }
        if (written == 0) {
            return 0;
        }
        sent += (size_t)written;
    }

    return 1;
}

static void send_response(int fd, int status_code, const char *status_text, const char *body) {
    char header[256];
    size_t body_length = body == NULL ? 0U : strlen(body);
    int header_length;

    header_length = snprintf(header, sizeof(header),
                             "HTTP/1.1 %d %s\r\n"
                             "Content-Type: application/json\r\n"
                             "Content-Length: %zu\r\n"
                             "Connection: close\r\n"
                             "\r\n",
                             status_code, status_text, body_length);
    if (header_length < 0) {
        return;
    }

    (void)send_all(fd, header, (size_t)header_length);
    if (body_length > 0U) {
        (void)send_all(fd, body, body_length);
    }
}

static void send_json_error(int fd, int status_code, const char *status_text, const char *message) {
    StrBuf body;

    sb_init(&body);
    if (!sb_append(&body, "{\"ok\":false,\"error\":") ||
        !sb_append_json_string(&body, message == NULL ? "unknown error" : message) ||
        !sb_append(&body, "}")) {
        sb_free(&body);
        send_response(fd, 500, "Internal Server Error",
                      "{\"ok\":false,\"error\":\"failed to build error response\"}");
        return;
    }

    send_response(fd, status_code, status_text, body.data);
    sb_free(&body);
}

static size_t find_header_end(const char *buffer, size_t length) {
    size_t i;

    if (length < 4U) {
        return (size_t)-1;
    }

    for (i = 0U; i + 3U < length; ++i) {
        if (buffer[i] == '\r' && buffer[i + 1U] == '\n' &&
            buffer[i + 2U] == '\r' && buffer[i + 3U] == '\n') {
            return i;
        }
    }

    return (size_t)-1;
}

static int parse_content_length(const char *value, size_t *out_length) {
    unsigned long parsed = 0UL;
    const unsigned char *cursor = (const unsigned char *)value;

    while (*cursor != '\0' && isspace(*cursor)) {
        cursor++;
    }
    if (*cursor == '\0') {
        return 0;
    }
    while (*cursor != '\0' && isdigit(*cursor)) {
        parsed = parsed * 10UL + (unsigned long)(*cursor - '0');
        cursor++;
    }
    while (*cursor != '\0' && isspace(*cursor)) {
        cursor++;
    }
    if (*cursor != '\0') {
        return 0;
    }
    *out_length = (size_t)parsed;
    return 1;
}

static int read_http_request(int fd, HttpRequest *request, char *errbuf, size_t errbuf_size) {
    char buffer[MAX_HTTP_REQUEST_SIZE + 1];
    size_t used = 0U;
    size_t header_end = (size_t)-1;
    size_t content_length = 0U;
    char *line_end;
    char *header_cursor;

    memset(request, 0, sizeof(*request));

    while (header_end == (size_t)-1) {
        ssize_t read_size;

        if (used == MAX_HTTP_REQUEST_SIZE) {
            snprintf(errbuf, errbuf_size, "HTTP request is too large");
            return 0;
        }

        read_size = recv(fd, buffer + used, MAX_HTTP_REQUEST_SIZE - used, 0);
        if (read_size < 0) {
            if (errno == EINTR) {
                continue;
            }
            snprintf(errbuf, errbuf_size, "failed to read HTTP request");
            return 0;
        }
        if (read_size == 0) {
            snprintf(errbuf, errbuf_size, "client closed the connection");
            return 0;
        }

        used += (size_t)read_size;
        buffer[used] = '\0';
        header_end = find_header_end(buffer, used);
    }

    line_end = strstr(buffer, "\r\n");
    if (line_end == NULL) {
        snprintf(errbuf, errbuf_size, "invalid HTTP request line");
        return 0;
    }
    *line_end = '\0';

    if (sscanf(buffer, "%7s %63s", request->method, request->path) != 2) {
        snprintf(errbuf, errbuf_size, "invalid HTTP request line");
        return 0;
    }

    header_cursor = line_end + 2;
    while ((size_t)(header_cursor - buffer) < header_end) {
        char *next = strstr(header_cursor, "\r\n");
        char *colon;
        size_t name_length;
        size_t value_length;
        const char *value;

        if (next == NULL || next == header_cursor) {
            break;
        }

        colon = memchr(header_cursor, ':', (size_t)(next - header_cursor));
        if (colon == NULL) {
            snprintf(errbuf, errbuf_size, "invalid HTTP header");
            return 0;
        }

        name_length = (size_t)(colon - header_cursor);
        value = colon + 1;
        while (value < next && isspace((unsigned char)*value)) {
            value++;
        }
        value_length = (size_t)(next - value);

        if (name_length == strlen("Content-Length") &&
            strncasecmp(header_cursor, "Content-Length", name_length) == 0) {
            char temp[32];

            if (value_length >= sizeof(temp)) {
                snprintf(errbuf, errbuf_size, "Content-Length header is too long");
                return 0;
            }
            memcpy(temp, value, value_length);
            temp[value_length] = '\0';
            if (!parse_content_length(temp, &content_length)) {
                snprintf(errbuf, errbuf_size, "invalid Content-Length header");
                return 0;
            }
        }

        header_cursor = next + 2;
    }

    if (content_length > MAX_HTTP_BODY_SIZE) {
        snprintf(errbuf, errbuf_size, "HTTP body is too large");
        return 0;
    }

    request->body = (char *)malloc(content_length + 1U);
    if (request->body == NULL) {
        snprintf(errbuf, errbuf_size, "out of memory while reading HTTP body");
        return 0;
    }

    {
        size_t body_offset = header_end + 4U;
        size_t body_bytes = used > body_offset ? used - body_offset : 0U;
        size_t copied = body_bytes > content_length ? content_length : body_bytes;

        if (copied > 0U) {
            memcpy(request->body, buffer + body_offset, copied);
        }
        request->body_length = copied;
    }

    while (request->body_length < content_length) {
        ssize_t read_size = recv(fd, request->body + request->body_length,
                                 content_length - request->body_length, 0);
        if (read_size < 0) {
            if (errno == EINTR) {
                continue;
            }
            free_http_request(request);
            snprintf(errbuf, errbuf_size, "failed to read HTTP body");
            return 0;
        }
        if (read_size == 0) {
            free_http_request(request);
            snprintf(errbuf, errbuf_size, "client closed the connection while sending the body");
            return 0;
        }
        request->body_length += (size_t)read_size;
    }

    request->body[request->body_length] = '\0';
    return 1;
}

static char *normalize_sql_copy(const char *sql) {
    char *copy;
    size_t len;

    copy = dup_string(sql);
    if (copy == NULL) {
        return NULL;
    }

    len = strlen(copy);
    while (len > 0U && isspace((unsigned char)copy[len - 1U])) {
        copy[--len] = '\0';
    }
    if (len > 0U && copy[len - 1U] == ';') {
        copy[--len] = '\0';
        while (len > 0U && isspace((unsigned char)copy[len - 1U])) {
            copy[--len] = '\0';
        }
    }
    return copy;
}

static int classify_sql(const char *sql, StatementType *out_type, char *errbuf, size_t errbuf_size) {
    char *normalized_sql;
    Statement stmt;
    int parsed;

    normalized_sql = normalize_sql_copy(sql);
    if (normalized_sql == NULL) {
        snprintf(errbuf, errbuf_size, "failed to copy SQL text");
        return 0;
    }
    if (normalized_sql[0] == '\0') {
        free(normalized_sql);
        snprintf(errbuf, errbuf_size, "SQL text is empty");
        return 0;
    }

    memset(&stmt, 0, sizeof(stmt));
    parsed = parse_statement(normalized_sql, &stmt);
    free(normalized_sql);

    if (!parsed) {
        if (stmt.type == STMT_UPDATE || stmt.type == STMT_DELETE) {
            snprintf(errbuf, errbuf_size, "only SELECT and INSERT are supported");
        } else {
            snprintf(errbuf, errbuf_size, "invalid SQL statement");
        }
        return 0;
    }
    if (stmt.type != STMT_SELECT && stmt.type != STMT_INSERT) {
        snprintf(errbuf, errbuf_size, "only SELECT and INSERT are supported");
        return 0;
    }

    *out_type = stmt.type;
    return 1;
}

static int build_select_response(const DbResult *result, StrBuf *body) {
    size_t i;

    if (!sb_append(body, "{\"ok\":true,\"type\":\"select\",\"rows\":[")) {
        return 0;
    }
    for (i = 0U; i < result->row_count; ++i) {
        if (i > 0U && !sb_append(body, ",")) {
            return 0;
        }
        if (!sb_append_json_string(body, result->rows[i])) {
            return 0;
        }
    }
    return sb_appendf(body, "],\"row_count\":%zu}", result->row_count);
}

static int build_insert_response(const DbResult *result, StrBuf *body) {
    return sb_appendf(body, "{\"ok\":true,\"type\":\"insert\",\"affected_rows\":%d}",
                      result->affected_rows);
}

static void handle_query_request(int fd, const char *body_text) {
    char *sql = NULL;
    char errbuf[512] = {0};
    char db_errbuf[512] = {0};
    StatementType stmt_type;
    DbResult result = {0};
    StrBuf response_body;
    int ok;

    if (!extract_sql_from_json(body_text, &sql, errbuf, sizeof(errbuf))) {
        send_json_error(fd, 400, "Bad Request", errbuf);
        return;
    }

    if (!classify_sql(sql, &stmt_type, errbuf, sizeof(errbuf))) {
        free(sql);
        send_json_error(fd, 422, "Unprocessable Entity", errbuf);
        return;
    }

    if (stmt_type == STMT_SELECT) {
        pthread_rwlock_rdlock(&g_db_lock);
    } else {
        pthread_rwlock_wrlock(&g_db_lock);
    }
    ok = db_execute_sql(sql, &result, db_errbuf, sizeof(db_errbuf));
    pthread_rwlock_unlock(&g_db_lock);
    free(sql);

    if (!ok) {
        send_json_error(fd, 422, "Unprocessable Entity",
                        db_errbuf[0] == '\0' ? "database execution failed" : db_errbuf);
        return;
    }

    sb_init(&response_body);
    if ((result.type == DB_RESULT_SELECT && !build_select_response(&result, &response_body)) ||
        (result.type == DB_RESULT_INSERT && !build_insert_response(&result, &response_body))) {
        sb_free(&response_body);
        db_free_result(&result);
        send_json_error(fd, 500, "Internal Server Error", "failed to build JSON response");
        return;
    }

    send_response(fd, 200, "OK", response_body.data);
    sb_free(&response_body);
    db_free_result(&result);
}

static void handle_client(int client_fd) {
    HttpRequest request;
    char errbuf[256] = {0};

    if (!read_http_request(client_fd, &request, errbuf, sizeof(errbuf))) {
        send_json_error(client_fd, 400, "Bad Request", errbuf);
        return;
    }

    if (strcmp(request.method, "GET") == 0 && strcmp(request.path, "/health") == 0) {
        send_response(client_fd, 200, "OK", "{\"ok\":true}");
        free_http_request(&request);
        return;
    }

    if (strcmp(request.method, "POST") == 0 && strcmp(request.path, "/query") == 0) {
        handle_query_request(client_fd, request.body == NULL ? "" : request.body);
        free_http_request(&request);
        return;
    }

    free_http_request(&request);
    send_json_error(client_fd, 404, "Not Found", "unsupported endpoint");
}

static int queue_init(JobQueue *queue, size_t capacity) {
    memset(queue, 0, sizeof(*queue));
    queue->items = (int *)calloc(capacity, sizeof(int));
    if (queue->items == NULL) {
        return 0;
    }
    queue->capacity = capacity;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    return 1;
}

static int queue_try_push(JobQueue *queue, int client_fd) {
    int pushed = 0;

    pthread_mutex_lock(&queue->mutex);
    if (queue->count < queue->capacity) {
        queue->items[queue->tail] = client_fd;
        queue->tail = (queue->tail + 1U) % queue->capacity;
        queue->count++;
        pushed = 1;
        pthread_cond_signal(&queue->not_empty);
    }
    pthread_mutex_unlock(&queue->mutex);
    return pushed;
}

static int queue_pop(JobQueue *queue) {
    int client_fd;

    pthread_mutex_lock(&queue->mutex);
    while (queue->count == 0U) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    client_fd = queue->items[queue->head];
    queue->head = (queue->head + 1U) % queue->capacity;
    queue->count--;
    pthread_mutex_unlock(&queue->mutex);
    return client_fd;
}

static void *worker_main(void *arg) {
    JobQueue *queue = (JobQueue *)arg;

    for (;;) {
        int client_fd = queue_pop(queue);
        handle_client(client_fd);
        close(client_fd);
    }

    return NULL;
}

static void print_usage(const char *program_name) {
    fprintf(stderr,
            "Usage: %s [--port PORT] [--workers N] [--queue N] [--data-dir PATH]\n",
            program_name);
}

static int parse_positive_int(const char *value, int *out_value) {
    char *end = NULL;
    long parsed = strtol(value, &end, 10);

    if (value == NULL || value[0] == '\0' || end == NULL || *end != '\0' || parsed <= 0L ||
        parsed > 65535L) {
        return 0;
    }
    *out_value = (int)parsed;
    return 1;
}

static int parse_args(int argc, char **argv, AppConfig *config) {
    int i;

    config->port = DEFAULT_PORT;
    config->worker_count = DEFAULT_WORKERS;
    config->queue_capacity = DEFAULT_QUEUE_CAPACITY;
    strncpy(config->data_dir, "data", sizeof(config->data_dir) - 1U);
    config->data_dir[sizeof(config->data_dir) - 1U] = '\0';

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--port") == 0) {
            if (i + 1 >= argc || !parse_positive_int(argv[i + 1], &config->port)) {
                return 0;
            }
            i++;
        } else if (strcmp(argv[i], "--workers") == 0) {
            if (i + 1 >= argc || !parse_positive_int(argv[i + 1], &config->worker_count)) {
                return 0;
            }
            i++;
        } else if (strcmp(argv[i], "--queue") == 0) {
            if (i + 1 >= argc || !parse_positive_int(argv[i + 1], &config->queue_capacity)) {
                return 0;
            }
            i++;
        } else if (strcmp(argv[i], "--data-dir") == 0) {
            if (i + 1 >= argc) {
                return 0;
            }
            strncpy(config->data_dir, argv[i + 1], sizeof(config->data_dir) - 1U);
            config->data_dir[sizeof(config->data_dir) - 1U] = '\0';
            i++;
        } else {
            return 0;
        }
    }

    return 1;
}

static int open_listen_socket(int port) {
    int listen_fd;
    int opt_value = 1;
    struct sockaddr_in addr;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        return -1;
    }
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt_value, sizeof(opt_value)) != 0) {
        close(listen_fd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(listen_fd);
        return -1;
    }
    if (listen(listen_fd, 128) != 0) {
        close(listen_fd);
        return -1;
    }

    return listen_fd;
}

int main(int argc, char **argv) {
    AppConfig config;
    pthread_t *workers = NULL;
    int listen_fd = -1;
    int i;

    signal(SIGPIPE, SIG_IGN);

    if (!parse_args(argc, argv, &config)) {
        print_usage(argv[0]);
        return 1;
    }

    if (chdir(config.data_dir) != 0) {
        fprintf(stderr, "failed to enter data directory '%s': %s\n",
                config.data_dir, strerror(errno));
        return 1;
    }

    if (!queue_init(&g_job_queue, (size_t)config.queue_capacity)) {
        fprintf(stderr, "failed to initialize the job queue\n");
        return 1;
    }

    workers = (pthread_t *)calloc((size_t)config.worker_count, sizeof(pthread_t));
    if (workers == NULL) {
        fprintf(stderr, "failed to allocate worker threads\n");
        return 1;
    }

    for (i = 0; i < config.worker_count; ++i) {
        if (pthread_create(&workers[i], NULL, worker_main, &g_job_queue) != 0) {
            fprintf(stderr, "failed to create worker thread %d\n", i);
            free(workers);
            return 1;
        }
    }

    listen_fd = open_listen_socket(config.port);
    if (listen_fd < 0) {
        fprintf(stderr, "failed to open listen socket on port %d\n", config.port);
        free(workers);
        return 1;
    }

    printf("listening on http://127.0.0.1:%d with %d worker(s)\n",
           config.port, config.worker_count);
    fflush(stdout);

    for (;;) {
        int client_fd = accept(listen_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "accept failed: %s\n", strerror(errno));
            break;
        }

        if (!queue_try_push(&g_job_queue, client_fd)) {
            send_json_error(client_fd, 503, "Service Unavailable", "request queue is full");
            close(client_fd);
        }
    }

    close(listen_fd);
    free(workers);
    return 1;
}

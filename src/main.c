#include <stdio.h>

#include "server/http_server.h"

int main(int argc, char **argv) {
    const char *port;
    const char *data_dir;

    if (argc < 2 || argc > 3) {
        fprintf(stderr, "usage: %s <port> [data_dir]\n", argv[0]);
        return 1;
    }

    port = argv[1];
    data_dir = argc == 3 ? argv[2] : "data";
    return http_server_run(port, data_dir);
}

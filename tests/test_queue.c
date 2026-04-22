#include <stdio.h>
#include <stdlib.h>

#include "../src/server/task_queue.h"

static void fail(const char *message) {
    fprintf(stderr, "test_queue: %s\n", message);
    exit(1);
}

int main(void) {
    TaskQueue queue;

    if (!task_queue_init(&queue, 2)) fail("task_queue_init failed");
    if (!task_queue_push(&queue, 10)) fail("first push failed");
    if (!task_queue_push(&queue, 11)) fail("second push failed");
    if (task_queue_push(&queue, 12)) fail("push should fail when queue is full");
    if (task_queue_size(&queue) != 2) fail("queue size should be 2");
    if (task_queue_pop(&queue) != 10) fail("first pop should return FIFO order");
    if (task_queue_pop(&queue) != 11) fail("second pop should return FIFO order");
    task_queue_shutdown(&queue);
    if (task_queue_pop(&queue) != -1) fail("shutdown queue should return -1 when empty");
    task_queue_destroy(&queue);
    return 0;
}

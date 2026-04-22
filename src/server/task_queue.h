#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include <pthread.h>

typedef struct Task {
    int fd;
    struct Task *next;
} Task;

typedef struct {
    Task *head;
    Task *tail;
    int size;
    int capacity;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} TaskQueue;

int task_queue_init(TaskQueue *queue, int capacity);
void task_queue_destroy(TaskQueue *queue);
int task_queue_push(TaskQueue *queue, int fd);
int task_queue_pop(TaskQueue *queue);
void task_queue_shutdown(TaskQueue *queue);
int task_queue_size(TaskQueue *queue);

#endif

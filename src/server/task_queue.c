#include "task_queue.h"

#include <stdlib.h>

int task_queue_init(TaskQueue *queue, int capacity) {
    if (!queue || capacity <= 0) return 0;
    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;
    queue->capacity = capacity;
    queue->shutting_down = 0;
    if (pthread_mutex_init(&queue->mutex, NULL) != 0) return 0;
    if (pthread_cond_init(&queue->cond, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        return 0;
    }
    return 1;
}

void task_queue_destroy(TaskQueue *queue) {
    Task *task;

    if (!queue) return;
    task = queue->head;
    while (task) {
        Task *next = task->next;
        free(task);
        task = next;
    }
    pthread_cond_destroy(&queue->cond);
    pthread_mutex_destroy(&queue->mutex);
}

int task_queue_push(TaskQueue *queue, int fd) {
    Task *task;

    if (!queue) return 0;
    pthread_mutex_lock(&queue->mutex);
    if (queue->shutting_down || queue->size >= queue->capacity) {
        pthread_mutex_unlock(&queue->mutex);
        return 0;
    }

    task = (Task *)calloc(1, sizeof(Task));
    if (!task) {
        pthread_mutex_unlock(&queue->mutex);
        return 0;
    }
    task->fd = fd;
    if (queue->tail) queue->tail->next = task;
    else queue->head = task;
    queue->tail = task;
    queue->size++;
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
    return 1;
}

int task_queue_pop(TaskQueue *queue) {
    Task *task;
    int fd;

    if (!queue) return -1;
    pthread_mutex_lock(&queue->mutex);
    while (!queue->shutting_down && queue->size == 0) {
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }
    if (queue->size == 0 && queue->shutting_down) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }

    task = queue->head;
    queue->head = task->next;
    if (!queue->head) queue->tail = NULL;
    queue->size--;
    fd = task->fd;
    free(task);
    pthread_mutex_unlock(&queue->mutex);
    return fd;
}

void task_queue_shutdown(TaskQueue *queue) {
    if (!queue) return;
    pthread_mutex_lock(&queue->mutex);
    queue->shutting_down = 1;
    pthread_cond_broadcast(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

int task_queue_size(TaskQueue *queue) {
    int size;

    if (!queue) return 0;
    pthread_mutex_lock(&queue->mutex);
    size = queue->size;
    pthread_mutex_unlock(&queue->mutex);
    return size;
}

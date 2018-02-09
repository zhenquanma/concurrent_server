#include "queue.h"
#include "csapp.h"

// #define INIT_QUEUE_SIZE 16;

queue_t *create_queue(void) {
    queue_t *queue = calloc(1, sizeof(queue_t));
    if(queue == NULL){
        return NULL;
    }
    if(pthread_mutex_init(&(queue->lock), NULL) != 0){
        return NULL;
    }
    Sem_init(&queue->items, 0, 0);
    queue->invalid = false;

    return queue;
}

bool invalidate_queue(queue_t *self, item_destructor_f destroy_function) {
    pthread_mutex_lock(&self->lock);
    if(self == NULL || self->invalid == true || destroy_function == NULL){
        errno = EINVAL;
        pthread_mutex_unlock(&self->lock);
        return false;
    }
    queue_node_t *node = self->front;
    queue_node_t *nodep;
    while(node != NULL) {
        destroy_function(node->item);
        nodep = node;
        node = node->next;
        free(nodep);
    }

    free(self->front);
    free(self->rear);
    self->invalid = true;
    pthread_mutex_unlock(&self->lock);
    return true;
}

bool enqueue(queue_t *self, void *item) {
    if(self == NULL || item == NULL) {
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&(self->lock));
    if(self->invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self->lock);
        return false;
    }
    queue_node_t *node = calloc(1, sizeof(queue_node_t));
    if(node == NULL){
        return false;
    }
    node->item = item;

    if(self->front == NULL) {
        self->front = node;
        self->rear = node;
    }
    else{
        self->rear->next = node;
        self->rear = node;
    }

    pthread_mutex_unlock(&self->lock);
    V(&self->items);
    return true;
}


void *dequeue(queue_t *self) {
    if(self == NULL){
        errno = EINVAL;
        return NULL;
    }
    P(&self->items);
    pthread_mutex_lock(&self->lock);
    if(self->invalid == true){
        errno = EINVAL;
        pthread_mutex_unlock(&self->lock);
        V(&self->items);
        return NULL;
    }
    if(self->front == self->rear){
        self->rear = NULL;
    }
    queue_node_t *node = self->front;
    self->front = node->next;
    pthread_mutex_unlock(&self->lock);
    return (void *)node;
}

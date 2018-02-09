#include "cream.h"
#include "utils.h"
#include "queue.h"
#include "csapp.h"
#include <sys/socket.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include "debug.h"

queue_t *request_queue;

hashmap_t *hashmap;

void *thread(void *vargp);
void usage(const char *arg);
void handle_request(int connfd);
void queue_free_function(void *item);
void map_free_function(map_key_t key, map_val_t val);


int main(int argc, char *argv[]) {
    int num_workers, max_entries, listenfd,  i;
    char *port_number;
    pthread_t tid;
    int *connfdp;

    signal(SIGPIPE, SIG_IGN);

    if(argc > 1) {
        int n;
        for(n = 1; n < argc; n++) {
            if(strcmp(argv[n], "-h") == 0){ /* Help menu */
                usage(argv[0]);
                exit(0);
            }
        }
    }
    if(argc != 4) {
        errno = EINVAL;
        fprintf(stderr, "Invalid arguments\n");
        exit(EXIT_FAILURE);
    }
    num_workers = atoi(argv[1]);
    port_number = argv[2];
    max_entries = atoi(argv[3]);

    if(num_workers <= 0 || max_entries <= 0){
        exit(EXIT_FAILURE);
    }

    debug("num_workers: %d", num_workers);
    debug("port_number: %s", port_number);
    debug("max_entries: %d", max_entries);

    listenfd = Open_listenfd(port_number);
    request_queue = create_queue();
    hashmap = create_map(max_entries, jenkins_one_at_a_time_hash, map_free_function);

    for(i = 0; i < num_workers; i++){
        Pthread_create(&tid, NULL, thread, NULL);
    }
    while(1) {
       connfdp = Calloc(1, sizeof(int));
       *connfdp = Accept(listenfd,NULL,NULL);
       enqueue(request_queue, connfdp);
    }
}

void *thread(void *vargp){
    while(1){
        queue_node_t *node = (queue_node_t *) dequeue(request_queue);
        int *connfd = (int *)(node->item);
        handle_request(*connfd);
        for(int i =0; i< hashmap->capacity; i++){
            printf("position %d: key: %s, val: %s, tombstone: %d\n",  i, (char *)(hashmap->nodes[i].key.key_base),(char *)(hashmap->nodes[i].val.val_base), hashmap->nodes[i].tombstone);
        }
        printf("\n");
        Close(*connfd);
    }
}

void usage(const char *arg) {
    printf("Usage:\n%s [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n" \
            "-h                 Displays this help menu and returns EXIT_SUCCESS.\n" \
            "NUM_WORKERS        The number of worker threads used to service requests.\n" \
            "PORT_NUMBER        Port number to listen on for incoming connections.\n" \
            "MAX_ENTRIES        The maximum number of entries that can be stored in `cream`'s underlying data store.\n", arg);
    return;
}

void handle_request(int connfd){
    // rio_t rio;
    request_header_t request_header;
    char *key_buf;
    char *value_buf;
    // Rio_readinitb(&rio, connfd);
    Rio_readn(connfd, &request_header, sizeof(request_header_t));

    /* Put request */
    if((request_header.request_code & PUT) == PUT) {
        if((request_header.key_size < MIN_KEY_SIZE || request_header.key_size > MAX_KEY_SIZE)
            && (request_header.value_size < MIN_VALUE_SIZE || request_header.value_size > MAX_VALUE_SIZE)){
            response_header_t response_header = {BAD_REQUEST, 0};
            Rio_writen(connfd, &response_header, sizeof(response_header_t));
            if(errno == EPIPE){
                Close(connfd);
            }
            return;
        }
        key_buf = Calloc(request_header.key_size, sizeof(char));
        value_buf = Calloc(request_header.value_size, sizeof(char));
        Rio_readn(connfd, key_buf, request_header.key_size);
        Rio_readn(connfd, value_buf, request_header.value_size);

        map_key_t key;
        map_val_t value;
        key.key_base = key_buf;
        key.key_len = request_header.key_size;
        value.val_base = value_buf;
        value.val_len = request_header.value_size;

        if(put(hashmap, key, value, true) == true){
            response_header_t response_header = {OK, 0};
            Rio_writen(connfd, &response_header,sizeof(response_header_t));
        }
        else{
            response_header_t response_header = {BAD_REQUEST, 0};
            Rio_writen(connfd, &response_header,sizeof(response_header_t));
        }
        if(errno == EPIPE){
            free(key_buf);
            free(value_buf);
            Close(connfd);
        }
    }


    /* Get request */
    else if((request_header.request_code & GET) == GET) {
        if(request_header.key_size < MIN_KEY_SIZE || request_header.key_size > MAX_KEY_SIZE) {
            response_header_t response_header = {BAD_REQUEST, 0};
            Rio_writen(connfd, &response_header, sizeof(response_header_t));
            if(errno == EPIPE){
                Close(connfd);
            }
            return;
        }
        key_buf = Calloc(request_header.key_size, sizeof(char));
        Rio_readn(connfd, key_buf, request_header.key_size);

        map_key_t key;
        key.key_base = key_buf;
        key.key_len = request_header.key_size;

        map_val_t value;
        value = get(hashmap, key);
        if(value.val_base == NULL){
            response_header_t response_header = {NOT_FOUND, 0};
            Rio_writen(connfd, &response_header, sizeof(response_header_t));
        }
        else{
            response_header_t response_header = {OK, value.val_len};
            Rio_writen(connfd, &response_header,sizeof(response_header_t));
            Rio_writen(connfd, value.val_base, value.val_len);
        }
        if(errno == EPIPE){
            Close(connfd);
        }
        free(key_buf);
    }


    /* Evict request */
    else if((request_header.request_code & EVICT) == EVICT) {
        if(request_header.key_size < MIN_KEY_SIZE || request_header.key_size > MAX_KEY_SIZE) {
            response_header_t response_header = {BAD_REQUEST, 0};
            Rio_writen(connfd, &response_header, sizeof(response_header_t));
            return;
        }
        key_buf = Calloc(request_header.key_size, sizeof(char));
        Rio_readn(connfd, key_buf, request_header.key_size);

        map_key_t key;
        key.key_base = key_buf;
        key.key_len = request_header.key_size;

        map_node_t node = delete(hashmap, key);
        if(node.key.key_base != NULL){
            hashmap->destroy_function(node.key, node.val);
            node.key.key_len = 0;
            node.val.val_len = 0;
        }
        response_header_t response_header = {OK, 0};
        Rio_writen(connfd, &response_header,sizeof(response_header));
        if(errno == EPIPE){
            Close(connfd);
        }
        free(key_buf);
    }


    /* Clear request */
    else if((request_header.request_code & CLEAR) == CLEAR) {
        clear_map(hashmap);
        response_header_t response_header = {OK, 0};
        Rio_writen(connfd, &response_header,sizeof(response_header_t));
        if(errno == EPIPE){
            Close(connfd);
        }
    }


    /* Invalid request */
    else{
        response_header_t response_header = {UNSUPPORTED, 0};
        Rio_writen(connfd, &response_header, sizeof(response_header_t));
        if(errno == EPIPE){
            Close(connfd);
        }
    }
    return;
}

void queue_free_function(void *item) {
    free(item);
}

void map_free_function(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}


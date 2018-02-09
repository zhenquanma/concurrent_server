#include "utils.h"
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include "debug.h"


void lock_writer(hashmap_t *self);
void unlock_writer(hashmap_t *self);
bool force_insert(hashmap_t *self, map_key_t key, map_val_t val, int index);
bool insert(hashmap_t *self, map_key_t key, map_val_t val, int index, int count, int candidate_i);
bool TTL_handle(hashmap_t *self, int index);

hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    if(hash_function == NULL || destroy_function == NULL){
        errno = EINVAL;
        return NULL;
    }
    hashmap_t *hashmap = calloc(1, sizeof(hashmap_t));
    if(hashmap == NULL){
        return NULL;
    }
    hashmap->capacity = capacity;
    hashmap->size = 0;
    hashmap->nodes = calloc(capacity, sizeof(map_node_t));
    if(hashmap->nodes == NULL){
        return NULL;
    }
    for(int i = 0; i < capacity; i++){
        hashmap->nodes[i].tombstone = false;
    }

    hashmap->hash_function = hash_function;
    hashmap->destroy_function = destroy_function;
    hashmap->num_readers = 0;
    if(pthread_mutex_init(&hashmap->write_lock, NULL) != 0) {
        return NULL;
    }
    if(pthread_mutex_init(&hashmap->fields_lock, NULL) !=0) {
        return NULL;
    }
    hashmap->invalid = false;
    return hashmap;
}


bool put(hashmap_t *self, map_key_t key, map_val_t val, bool force) {
    pthread_mutex_lock(&self->write_lock);
    if(self == NULL || self->invalid == true || key.key_base == NULL || key.key_len == 0 ||
                    val.val_base == NULL || val.val_len == 0) {
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }
    int index = get_index(self, key);
    debug("After hash index = %d", index);

    if(self->size == self->capacity){
        if(force == true){
            force_insert(self, key, val, index);
            pthread_mutex_unlock(&self->write_lock);
            return true;
        }
        else{
            errno = ENOMEM;
            pthread_mutex_unlock(&self->write_lock);
            return false;
        }
    }
    else{ /* Map is not full */
        insert(self, key, val, index, 0, -1);
        pthread_mutex_unlock(&self->write_lock);
        return true;
    }
}



map_val_t get(hashmap_t *self, map_key_t key) {
    lock_writer(self);
    if(self == NULL || self->invalid == true || key.key_base == NULL || key.key_len == 0) {
        errno = EINVAL;
        unlock_writer(self);
        return MAP_VAL(NULL, 0);
    }
    int index = get_index(self, key);
    if(self->nodes[index].tombstone == false) {
        TTL_handle(self, index);
         if(self->nodes[index].key.key_base != NULL && self->nodes[index].key.key_len == key.key_len
            && memcmp(self->nodes[index].key.key_base, key.key_base, key.key_len) == 0) {
            unlock_writer(self);

            debug("Get from Index: %d", index);
            debug("Key: %s", (char*)(self->nodes[index].key.key_base));

            return self->nodes[index].val;
        }
    }

    int i = index + 1;
    TTL_handle(self, i);
    while(i != index) {
        if(self->nodes[i].tombstone == true)
            i++;
        else if(self->nodes[i].key.key_base != NULL) {
            if(self->nodes[i].key.key_len == key.key_len &&
                memcmp(self->nodes[i].key.key_base, key.key_base, key.key_len) == 0) {
                unlock_writer(self);

                debug("Get from Index(i) : %d", i);
                debug("Key: %s", (char*)(self->nodes[i].key.key_base));

                return self->nodes[i].val;
            }
            else {
                i++;
            }
        }
        if(i == self->capacity){
            i = 0;
        }
    }
    unlock_writer(self);
    return MAP_VAL(NULL, 0);
}


map_node_t delete(hashmap_t *self, map_key_t key) {
    lock_writer(self);

    if(self == NULL || self->invalid == true || key.key_base == NULL || key.key_len == 0) {
        errno = EINVAL;
        unlock_writer(self);
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    int index = get_index(self, key);

    TTL_handle(self, index);
    if(self->nodes[index].key.key_base != NULL && self->nodes[index].key.key_len == key.key_len &&
        memcmp(self->nodes[index].key.key_base, key.key_base, key.key_len) == 0) {
        if(self->nodes[index].tombstone == false) {
            self->size--;
            self->nodes[index].tombstone = true;
            unlock_writer(self);
            return self->nodes[index];
        }
    }

    int i = index + 1;
    while(i != index){
        TTL_handle(self, i);
        if(self->nodes[index].key.key_base != NULL){
            if(self->nodes[i].tombstone == true)
                i++;
            else {
                if(self->nodes[i].key.key_len == key.key_len &&
                    memcmp(self->nodes[i].key.key_base, key.key_base, key.key_len) == 0) {
                    self->nodes[i].tombstone = true;
                    self->size--;
                    unlock_writer(self);
                    return self->nodes[i];
                }
                else {
                    i++;
                }
            }
        }
        else{
            i++;
        }
        if(i == self->capacity){
            i = 0;
        }
    }
    unlock_writer(self);
    return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
}


bool clear_map(hashmap_t *self) {
    lock_writer(self);
    if(self == NULL || self->invalid == true){
        errno = EINVAL;
        unlock_writer(self);
        return false;
    }
    int i;
    for(i = 0; i < self->capacity; i++) {
        if(self->nodes[i].key.key_base != NULL && self->nodes[i].tombstone == false){
            self->destroy_function(self->nodes[i].key, self->nodes[i].val);
            self->nodes[i].key.key_len = 0;
            self->nodes[i].val.val_len = 0;
        }
    }
    self->size = 0;
    unlock_writer(self);
    return true;
}


bool invalidate_map(hashmap_t *self) {
    lock_writer(self);
    if(self == NULL || self->invalid == true){
        errno = EINVAL;
        unlock_writer(self);
        return false;
    }
    int i;
    for(i = 0; i < self->capacity; i++) {
        if(self->nodes[i].key.key_base != NULL && self->nodes[i].tombstone == false)
            self->destroy_function(self->nodes[i].key, self->nodes[i].val);
    }
    free(self->nodes);
    self->invalid = true;
    unlock_writer(self);
    return true;
}

void lock_writer(hashmap_t *self){
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers++;
    if(self->num_readers == 1){
        pthread_mutex_lock(&self->write_lock);
    }
    pthread_mutex_unlock(&self->fields_lock);
}

void unlock_writer(hashmap_t *self){
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers--;
    if(self->num_readers == 0){
        pthread_mutex_unlock(&self->write_lock);
    }
    pthread_mutex_unlock(&self->fields_lock);
}

bool force_insert(hashmap_t *self, map_key_t key, map_val_t val, int index) {
    if(self->nodes[index].key.key_base != NULL && self->nodes[index].tombstone == false)
        self->destroy_function(self->nodes[index].key, self->nodes[index].val);
    self->nodes[index].key = key;
    self->nodes[index].val = val;
    self->nodes[index].tombstone = false;
    self->nodes[index].start = clock();
    debug("key: %s", (char*)(self->nodes[index].key.key_base));
    debug("Force put in index: %d", index);
    return true;
}


bool insert(hashmap_t *self, map_key_t key, map_val_t val, int index, int count, int candidate_i){
    if(count == self->capacity){ /* No suitable place after wrap around*/
        if(candidate_i >= 0){
            self->destroy_function(self->nodes[candidate_i].key, self->nodes[candidate_i].val);
            self->nodes[candidate_i].key = key;
            self->nodes[candidate_i].val = val;
            self->size++;
            self->nodes[candidate_i].tombstone = false;
            self->nodes[candidate_i].start = clock();
            debug("Key: %s", (char*)(self->nodes[index].key.key_base));
            debug("Put in index(tombstone): %d", candidate_i);
            return true;
        }
        return false;
    }

    if(self->nodes[index].tombstone == true){ /* keep serching a position with same key first, then go back if it cannot be found */
        if(candidate_i < 0){
            candidate_i = index;
        }
    }
    else{
        TTL_handle(self, index);
    }
    if(self->nodes[index].key.key_base == NULL){ /* empty node */
        self->nodes[index].key = key;
        self->nodes[index].val = val;
        self->size++;
        self->nodes[index].start = clock();
        debug("Key: %s", (char*)(self->nodes[index].key.key_base));
        debug("Put in index(empty): %d", index);
        return true;
    }
    else if(self->nodes[index].key.key_len == key.key_len &&
            memcmp(self->nodes[index].key.key_base, key.key_base, key.key_len) == 0) { /* key exists */
        self->destroy_function(self->nodes[index].key, self->nodes[index].val);
        self->nodes[index].key = key;
        self->nodes[index].val = val;
        if(self->nodes[index].tombstone == true){
            self->nodes[index].tombstone = false;
            self->size++;
        }
        self->nodes[index].start = clock();
        debug("Key: %s", (char*)(self->nodes[index].key.key_base));
        debug("Put in index: %d", index);
        return true;
    }
    else {
        int i = index + 1;
        if(i == self->capacity){
            i = 0;
        }
        if(insert(self, key, val, i, count + 1, candidate_i) == true) {
            return true;
        }
        else {
            return false;
        }
    }
}

double get_time_elapsed(clock_t start){
    return ((double) (clock() - start)) / CLOCKS_PER_SEC;
}

bool TTL_handle(hashmap_t *self, int index){
    double i = get_time_elapsed(self->nodes[index].start);
    printf("i = %f\n", i);
    if( i - TTL > 0){
        self->destroy_function(self->nodes[index].key, self->nodes[index].val);
        self->nodes[index].start = 0;
        return true;
    }
    return false;
}
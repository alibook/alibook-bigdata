#include <stdio.h>
#include <string.h>
#include <libmemcached/memcached.h>

#define TARGET_HOST  "5fe328398ee149a2.m.cnqdalicm9pub001.ocs.aliyuncs.com"

int main(int argc, char *argv[])
{
    memcached_st *memc = NULL;
    memcached_return_t rc;
    memcached_server_st *server;
    memc = memcached_create(NULL);
    server = memcached_server_list_append(NULL, TARGET_HOST, 11211,&rc);

    rc = memcached_server_push(memc,server);
    if (rc != MEMCACHED_SUCCESS) {
      fprintf(stderr, "Failed to connect MemCached server, error: %d\n", rc);
    }
    memcached_server_list_free(server);

    const char *key = "TestKey";
    const char *value = "TestValue";
    size_t value_length = strlen(value);
    size_t key_length = strlen(key);
    int expiration = 0;
    uint32_t flags = 0;

    /* Save data */
    fprintf(stdout, "=======================================\n");
    rc = memcached_set(memc, key, key_length, value, value_length, expiration, flags);
    if (rc != MEMCACHED_SUCCESS) {
      fprintf(stderr, "Save data failed: %d\n", rc);
      return -1;
    }
    fprintf(stdout, "Save data succeed, key: %s value: %s\n" ,key, value);

    /* Get data */
    fprintf(stdout, "=======================================\n");
    fprintf(stdout, "Start get key: %s\n", key);
    char* result = memcached_get(memc, key, key_length, &value_length, &flags, &rc);
    fprintf(stdout, "Get value: %s\n", result);

    /* Get data which doesn't exist */
    fprintf(stdout, "=======================================\n");
    const char *key_a = "TestKey2";
    size_t     key_a_length = strlen(key_a);
    fprintf(stdout, "Start get key: %s\n", key_a);
    result = memcached_get(memc, key_a, key_a_length, &value_length, &flags, &rc);
    fprintf(stdout, "Get value: %s\n", result);

    /* Delete data */
    fprintf(stdout, "=======================================\n");
    fprintf(stdout, "Start delete key: %s\n", key);
    rc = memcached_delete(memc,key, key_length, expiration);
    if (rc != MEMCACHED_SUCCESS) {
      fprintf(stderr, "Delete key failed: %s\n", rc);
    }
    fprintf(stdout, "Delete key succeed\n");
    
    /* free */
    memcached_free(memc);
    return 0;
}

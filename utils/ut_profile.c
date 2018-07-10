/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include <ctype.h>
# include <stdlib.h>
# include <string.h>
# include <sys/types.h>
# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>
# include <netdb.h> 
# include <jansson.h>

# include "ut_dict.h"
# include "nw_timer.h"

static int sockfd;
static char *agent_host = "127.0.0.1";
static int agent_port = 8888;
static struct sockaddr_in agent_addr;
static char *process_scope;
static char *process_host;
static dict_t *dict_profile;
static nw_timer timer;

struct profile_val {
    uint64_t val;
};

static char *escape_key(const char *key)
{
    char *new_key = strdup(key);
    size_t len = strlen(key);
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(new_key[i]) || new_key[i] == '.' || new_key[i] == '-' || new_key[i] == '_')) {
            new_key[i] = '_';
        }
    }
    return new_key;
}

static uint32_t dict_profile_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int dict_profile_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *dict_profile_key_dup(const void *key)
{
    return strdup(key);
}

static void dict_profile_key_free(void *key)
{
    free(key);
}

static void *dict_profile_val_dup(const void *val)
{
    struct profile_val *obj = malloc(sizeof(struct profile_val));
    memcpy(obj, val, sizeof(struct profile_val));
    return obj;
}

static void dict_profile_val_free(void *val)
{
    free(val);
}

static void report_profile(const char *key, uint64_t val)
{
    if (sockfd == 0)
        return;

    char *new_key = escape_key(key);
    json_t *params = json_array();
    json_array_append_new(params, json_string(process_scope));
    json_array_append_new(params, json_string(new_key));
    json_array_append_new(params, json_string(process_host));
    json_array_append_new(params, json_integer(val));

    json_t *message = json_object();
    json_object_set_new(message, "method", json_string("monitor.inc"));
    json_object_set_new(message, "params", params);

    char *message_str = json_dumps(message, 0);
    size_t message_len = strlen(message_str);
    message_str[message_len] = '\n';
    sendto(sockfd, message_str, message_len + 1, 0, (struct sockaddr *)&agent_addr, sizeof(agent_addr));

    json_decref(message);
    free(message_str);
    free(new_key);
}

static void on_timer(nw_timer *timer, void *privdata)
{
    if (dict_size(dict_profile) == 0)
        return;
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_profile);
    while ((entry = dict_next(iter)) != NULL) {
        struct profile_val *obj = entry->val;
        report_profile(entry->key, obj->val);
    }
    dict_release_iterator(iter);
    dict_clear(dict_profile);
}

int profile_init(const char *scope, const char *host)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = dict_profile_hash_func;
    type.key_compare    = dict_profile_key_compare;
    type.key_dup        = dict_profile_key_dup;
    type.key_destructor = dict_profile_key_free;
    type.val_dup        = dict_profile_val_dup;
    type.val_destructor = dict_profile_val_free;

    dict_profile = dict_create(&type, 64);
    if (dict_profile == NULL)
        return -__LINE__;

    memset(&agent_addr, 0, sizeof(agent_addr));
    agent_addr.sin_family = AF_INET;
    inet_aton(agent_host, &agent_addr.sin_addr);
    agent_addr.sin_port = htons(agent_port);

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)
        return -__LINE__;

    process_scope = strdup(scope);
    process_host  = strdup(host);

    nw_timer_set(&timer, 10, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

void profile_set(const char *key, uint64_t val)
{
    if (sockfd == 0)
        return;

    char *new_key = escape_key(key);
    json_t *params = json_array();
    json_array_append_new(params, json_string(process_scope));
    json_array_append_new(params, json_string(new_key));
    json_array_append_new(params, json_string(process_host));
    json_array_append_new(params, json_integer(val));

    json_t *message = json_object();
    json_object_set_new(message, "method", json_string("monitor.set"));
    json_object_set_new(message, "params", params);

    char *message_str = json_dumps(message, 0);
    size_t message_len = strlen(message_str);
    message_str[message_len] = '\n';
    sendto(sockfd, message_str, message_len + 1, 0, (struct sockaddr *)&agent_addr, sizeof(agent_addr));

    json_decref(message);
    free(message_str);
    free(new_key);
}

void profile_inc(const char *key, uint64_t val)
{
    dict_entry *entry = dict_find(dict_profile, key);
    if (entry) {
        struct profile_val *obj = entry->val;
        obj->val += val;
    } else {
        struct profile_val obj = { .val = val };
        dict_add(dict_profile, (void *)key, &obj);
    }
}

void profile_inc_real(const char *key, uint64_t val)
{
    if (sockfd == 0)
        return;

    char *new_key = escape_key(key);
    json_t *params = json_array();
    json_array_append_new(params, json_string(process_scope));
    json_array_append_new(params, json_string(new_key));
    json_array_append_new(params, json_string(process_host));
    json_array_append_new(params, json_integer(val));

    json_t *message = json_object();
    json_object_set_new(message, "method", json_string("monitor.inc"));
    json_object_set_new(message, "params", params);

    char *message_str = json_dumps(message, 0);
    size_t message_len = strlen(message_str);
    message_str[message_len] = '\n';
    sendto(sockfd, message_str, message_len + 1, 0, (struct sockaddr *)&agent_addr, sizeof(agent_addr));

    json_decref(message);
    free(message_str);
    free(new_key);
}


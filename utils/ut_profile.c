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

static int sockfd;
static char *agent_host = "127.0.0.1";
static int agent_port = 8888;
static struct sockaddr_in agent_addr;
static char *process_scope;
static char *process_host;

int profile_init(const char *scope, const char *host)
{
    memset(&agent_addr, 0, sizeof(agent_addr));
    agent_addr.sin_family = AF_INET;
    inet_aton(agent_host, &agent_addr.sin_addr);
    agent_addr.sin_port = htons(agent_port);

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)
        return -__LINE__;

    process_scope = strdup(scope);
    process_host  = strdup(host);

    return 0;
}

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

void profile_inc(const char *key, uint64_t val)
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


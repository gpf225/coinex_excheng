/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include "ut_http.h"

int parse(char *url)
{
    http_params_t *obj = http_parse_url_params(url);
    if (obj == NULL) {
        printf("parse fail, url: %s\n", url);
        return 0;
    }

    printf("path: %s\n", obj->path);
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(obj->params);
    while ((entry = dict_next(iter))) {
        printf("%s : %s\n", (char *)entry->key, (char *)entry->val);
    }
    http_params_release(obj);

    return 0;
}

int main(void)
{
    char *line = NULL;
    size_t buf_size = 0;

    while (getline(&line, &buf_size, stdin) != -1)
    {
        size_t len = strlen(line);
        if (len && line[len - 1] == '\n')
            line[--len] = 0;

        parse(line);
    }

    return 0;
}


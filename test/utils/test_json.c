/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <jansson.h>
# include "ut_dict.h"
# include "ut_misc.h"


int main(void)
{
    json_t **jsons = (json_t **)malloc(sizeof(json_t *) * 12000);
    for (int j = 0; j < 12000; j++) {
        jsons[j] = json_object();
        for (int m = 0; m < 100; m++) {
            char key[10] = {0};
            sprintf(key, "key%d", m);
            json_object_set_new(jsons[j], key, json_integer(m));
        }
    }

    double start = current_timestamp();
    for (int j = 0; j < 12000; j++) {
        json_decref(jsons[j]);
    }
    double end = current_timestamp();
    printf("cost: %lf\n", end - start);

    json_t *array1 = json_array();
    json_array_append_new(array1, json_string("test11"));
    json_array_append_new(array1, json_string("test12"));
    json_array_append_new(array1, json_string("test13"));
    json_array_insert(array1, 0, json_string("test14"));

    /*
    json_t *array2 = json_array();
    json_array_append_new(array2, json_string("test21"));
    json_array_append_new(array2, json_string("test22"));
    json_array_append_new(array2, json_string("test23"));

    json_array_extend(array1, array2);
    json_decref(array2);
    */
    for(size_t i = 0; i < json_array_size(array1); i++) {
        printf("%s\n", json_string_value(json_array_get(array1, i)));
    }
    //json_decref(array2);
    return 0;
}


/*
 * Copyright (c) 2018, Haipo Yang <yang@haipo.me>
 * All rights reserved.
 */

# include "aw_index.h"
# include "aw_server.h"

static dict_t *dict_session;

int init_index(void)
{
    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function = ptr_dict_hash_func;
    dt.key_compare   = ptr_dict_key_compare;

    dict_session = dict_create(&dt, 1024);
    if (dict_session == NULL)
        return -__LINE__;

    return 0;
}

int index_subscribe(nw_ses *ses)
{
    dict_replace(dict_session, ses, NULL);
    return 0;
}

int index_unsubscribe(nw_ses *ses)
{
    dict_delete(dict_session, ses);
    return 0;
}

int index_on_update(const char *market, const char *price)
{
    json_t *params = json_array();
    json_array_append_new(params, json_string(market));
    json_array_append_new(params, json_string(price));

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_session);
    while ((entry = dict_next(iter)) != NULL) {
        send_notify(entry->key, "index.update", params);
    }
    dict_release_iterator(iter);

    json_decref(params);
    return 0;
}

size_t index_subscribe_number(void)
{
    return dict_size(dict_session);
}


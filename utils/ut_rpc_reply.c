/*
 * Description: 
 *     History: zhoumugui@viabtc.com, 2019/02/02, create
 */

# include "ut_rpc_reply.h"

# include <stdlib.h>
# include <string.h>

static const int CODE_REPLY_OK      = 0;
static const int CODE_REPLY_INVALID = -99999999;

bool reply_ok(ut_rpc_reply_t *reply)
{
    return reply->code == CODE_REPLY_OK;
}

bool reply_valid(ut_rpc_reply_t *reply)
{
    return reply->code != CODE_REPLY_INVALID;
}

ut_rpc_reply_t *reply_create(json_t *reply)
{
    ut_rpc_reply_t *rpc_reply = malloc(sizeof(ut_rpc_reply_t));
    if (rpc_reply == NULL) {
        return NULL;
    }
    memset(rpc_reply, 0, sizeof(ut_rpc_reply_t));

    rpc_reply->id = json_integer_value(json_object_get(reply, "id"));
    json_t *error = json_object_get(reply, "error");
    json_t *result = json_object_get(reply, "result");
    if (error == NULL || result == NULL) {
        rpc_reply->code = CODE_REPLY_INVALID;
        rpc_reply->message = strdup("error field or result field does not exists");
        return rpc_reply;
    }

    if (json_is_null(error) && json_is_null(result)) {
        rpc_reply->code = CODE_REPLY_INVALID;
        rpc_reply->message = strdup("error and result should not be null at the same time");
        return rpc_reply;
    }

    if (!json_is_null(error)) {
        if (!json_is_integer(json_object_get(error, "code"))) {
            rpc_reply->code = CODE_REPLY_INVALID;
            rpc_reply->message = strdup("no code field or code is not an integer number");
            return rpc_reply;
        }

        if (!json_is_string(json_object_get(error, "message"))) {
            rpc_reply->code = CODE_REPLY_INVALID;
            rpc_reply->message = strdup("no message field or message is not a string");
            return rpc_reply;
        }

        rpc_reply->code = json_integer_value(json_object_get(error, "code"));
        rpc_reply->message = strdup(json_string_value(json_object_get(error, "message")));

        if (rpc_reply->code == CODE_REPLY_OK) {
             rpc_reply->code = CODE_REPLY_INVALID;
             free(rpc_reply->message);
             rpc_reply->message = strdup("this is an error reply, but reply code means success");
        }
        
        return rpc_reply;
    }
    
    rpc_reply->result = result;
    json_incref(rpc_reply->result);
    rpc_reply->code = CODE_REPLY_OK;
    rpc_reply->message = strdup("Ok");
    return rpc_reply;
}

ut_rpc_reply_t* reply_load(const void *json_data, uint32_t data_size)
{
    json_t *reply = json_loadb(json_data, data_size, 0, NULL);
    if (reply == NULL) {
        ut_rpc_reply_t *rpc_reply = malloc(sizeof(ut_rpc_reply_t));
        memset(rpc_reply, 0, sizeof(ut_rpc_reply_t));

        rpc_reply->code = CODE_REPLY_INVALID;
        rpc_reply->message = strdup("data not a json string");
        return rpc_reply;
    }

    ut_rpc_reply_t *rpc_reply = reply_create(reply);
    json_decref(reply);

    return rpc_reply;
}

void reply_release(ut_rpc_reply_t *reply)
{
    if (reply->message != NULL) {
        free(reply->message);
    }

    if (reply->result != NULL) {
        json_decref(reply->result);
    }

    free(reply);

    return ;
}

json_t* reply_get_result_json(int id, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set_new(reply, "result", result);
    json_object_set_new(reply, "id", json_integer(id));

    return reply;
}

json_t* reply_get_error_json(int id, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(id));

    return reply;
}


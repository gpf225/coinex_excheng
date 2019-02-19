# ifndef _TEST_UT_RPC_REPLY_H_
# define _TEST_UT_RPC_REPLY_H_

# include "../../utils/ut_rpc_reply.h"
# include <stdarg.h>
# include <stddef.h>
# include <setjmp.h>
# include <google/cmockery.h>
# include <string.h>

# include "ut_sds.h"
# include "ut_log.h"
# include "ut_rpc.h"
# include "ut_misc.h"

static json_t* get_error_json(int code, const char *message)
{
    json_t *error_json = json_object();
    json_object_set_new(error_json, "code", json_integer(code));
    json_object_set_new(error_json, "message", json_string("test_error"));
    return error_json;
}

void test_reply_create_ok_case1(void **state)
{
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", json_null());
    json_object_set_new(reply_json, "result", json_string("test_result"));
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_true(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_int_equal(reply->code, 0);
    assert_string_equal(reply->message, "Ok");
    assert_true(reply->result != NULL);
    assert_string_equal("test_result", json_string_value(reply->result));

    reply_release(reply);
}

void test_reply_create_ok_case2(void **state)
{
    // error域不为空时表示响应失败，此时result域信息将会被忽略。
    json_t *error_json = get_error_json(11, "test_error");
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", error_json);
    json_object_set_new(reply_json, "result", json_string("test_result"));
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_int_equal(reply->code, 11);
    assert_string_equal(reply->message, "test_error");
    assert_true(reply->result == NULL);

    reply_release(reply);
}

void test_reply_create_error_case1(void **state)
{
    json_t *error_json = get_error_json(11, "test_error");
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", error_json);
    json_object_set_new(reply_json, "result", json_null());
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_int_equal(reply->code, 11);
    assert_string_equal(reply->message, "test_error");
    assert_true(reply->result == NULL);

    reply_release(reply);
}


void test_reply_create_invalid_case1(void **state)
{
    // error和result域同时为空
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", json_null());
    json_object_set_new(reply_json, "result", json_null());
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_false(reply_valid(reply));
    assert_true(reply->result == NULL);

    reply_release(reply);
}

void test_reply_create_invalid_case2(void **state)
{
    // result域不存在
    json_t *error_json = get_error_json(11, "test_error");
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", error_json);
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_false(reply_valid(reply));
    assert_true(reply->result == NULL);

    reply_release(reply);
}

void test_reply_create_invalid_case3(void **state)
{
    // error域不存在
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "result", json_string("test"));
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_false(reply_valid(reply));
    assert_true(reply->result == NULL);

    reply_release(reply);
}

void test_reply_create_invalid_case4(void **state)
{
    // error域message字段不存在
    json_t *error_json = json_object();
    json_object_set_new(error_json, "code", json_integer(11));

    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", error_json);
    json_object_set_new(reply_json, "result", json_null());
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_false(reply_valid(reply));
    assert_true(reply->result == NULL);

    reply_release(reply);
}

void test_reply_create_invalid_case5(void **state)
{
    // error域code字段不存在
    json_t *error_json = json_object();
    json_object_set_new(error_json, "message", json_string("test_error"));

    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", error_json);
    json_object_set_new(reply_json, "result", json_null());
    json_object_set_new(reply_json, "id", json_integer(11));

    ut_rpc_reply_t *reply = reply_create(reply_json);
    json_decref(reply_json);

    assert_false(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_false(reply_valid(reply));
    assert_true(reply->result == NULL);

    reply_release(reply);
}

void test_reply_reload_ok_case1(void **state)
{
    json_t *reply_json = json_object();
    json_object_set_new(reply_json, "error", json_null());
    json_object_set_new(reply_json, "result", json_string("test_result"));
    json_object_set_new(reply_json, "id", json_integer(11));
    
    char *json_str = json_dumps(reply_json, 0);
    ut_rpc_reply_t *reply = reply_load(json_str, strlen(json_str));
    json_decref(reply_json);

    assert_true(reply_ok(reply));
    assert_int_equal(reply->id, 11);
    assert_int_equal(reply->code, 0);
    assert_string_equal(reply->message, "Ok");
    assert_true(reply->result != NULL);
    assert_string_equal("test_result", json_string_value(reply->result));
  
    free(json_str);
    reply_release(reply);
}

void test_reply_reload_invalid_case1(void **state)
{
    const char *str = "This is not a json";
    ut_rpc_reply_t *reply = reply_load(str, strlen(str));

    assert_false(reply_valid(reply));
    assert_true(reply->result == NULL);
    reply_release(reply);
}

void test_reply_result_json(void **state)
{
    json_t *json1 = json_object();
    json_object_set_new(json1, "error", json_null());
    json_object_set_new(json1, "result", json_string("test_result"));
    json_object_set_new(json1, "id", json_integer(11));

    json_t *json2 = reply_get_result_json(11, json_string("test_result"));

    char *json1_str = json_dumps(json1, JSON_SORT_KEYS);
    char *json2_str = json_dumps(json2, JSON_SORT_KEYS);
    assert_string_equal(json1_str, json2_str);

    json_decref(json1);
    json_decref(json2);
    free(json1_str);
    free(json2_str);

}

void test_reply_error_json(void **state)
{
    json_t *error_json = get_error_json(12, "test_error");
    json_t *json1 = json_object();
    json_object_set_new(json1, "error", error_json);
    json_object_set_new(json1, "result", json_null());
    json_object_set_new(json1, "id", json_integer(11));

    json_t *json2 = reply_get_error_json(11, 12, "test_error");

    char *json1_str = json_dumps(json1, JSON_SORT_KEYS);
    char *json2_str = json_dumps(json2, JSON_SORT_KEYS);
    assert_string_equal(json1_str, json2_str);

    json_decref(json1);
    json_decref(json2);
    free(json1_str);
    free(json2_str);
}

static struct nw_ses* test_create_ses()
{
    const char *cfg = "tcp@127.0.0.1:8080"; 
    nw_ses *ses = malloc(sizeof(nw_ses));   
    memset(ses, 0, sizeof(nw_ses));
    int sock_type = 0;
    int ret = nw_sock_cfg_parse(cfg, &ses->peer_addr, &sock_type);
    if (ret != 0) {
        return NULL;
    }
    return ses;
}

void test_reply_macro_log(void **state)
{
    const char *data = "{\"key\": 1121}";
    struct rpc_pkg pkg;
    memset(&pkg, 0, sizeof(rpc_pkg));
    pkg.body = strdup(data);
    pkg.body_size = strlen(data);
    
    struct nw_ses *ses = test_create_ses();
    struct rpc_pkg *pkg_p = &pkg;
    REPLY_TRACE_LOG(ses, pkg_p);
    REPLY_INVALID_LOG(ses, pkg_p);
    REPLY_ERROR_LOG(ses, pkg_p);

    free(pkg.body);
    free(ses);
}

int main()
{
    const UnitTest tests[] = {  
        unit_test(test_reply_create_ok_case1),
        unit_test(test_reply_create_ok_case2),
        unit_test(test_reply_create_error_case1),
        unit_test(test_reply_create_invalid_case1),
        unit_test(test_reply_create_invalid_case2),
        unit_test(test_reply_create_invalid_case3),
        unit_test(test_reply_create_invalid_case4),
        unit_test(test_reply_create_invalid_case5),
        unit_test(test_reply_reload_ok_case1),
        unit_test(test_reply_reload_invalid_case1),
        unit_test(test_reply_result_json),
        unit_test(test_reply_error_json),
        unit_test(test_reply_macro_log)
    }; 
    
    return run_tests(tests);  
}

# endif
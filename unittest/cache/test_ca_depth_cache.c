# include "../../cache/ca_depth_cache.h"

# include <stdarg.h>
# include <stddef.h>
# include <setjmp.h>
# include <google/cmockery.h>

# include "ut_misc.h"

static const double cache_timeout = 0.5;

static json_t *get_json(const char *market, const char *interval, int limit)
{
    json_t *data = json_object();
    json_object_set_new(data, "market", json_string(market));
    json_object_set_new(data, "interval", json_string(interval));
    json_object_set_new(data, "limit", json_integer(limit));
    return data;
}

static bool cache_not_expired(struct depth_cache_val *val)
{
    double cur = current_timestamp();
    return cur - val->time < 2 * cache_timeout;
}

static bool last_limit_not_expired(struct depth_cache_val *val)
{
    double cur = current_timestamp();
    return cur - val->limit_last_hit_time < 10 * cache_timeout;
}

static void expire_limit(struct depth_cache_val *val)
{
    val->limit_last_hit_time -= 11 * cache_timeout;
}

static bool is_json_equal(json_t *json1, json_t *json2)
{
    char *json1_str = json_dumps(json1, JSON_SORT_KEYS);
    char *json2_str = json_dumps(json2, JSON_SORT_KEYS);

    int ret = strcmp(json1_str, json2_str);

    free(json1_str);
    free(json2_str);

    return ret == 0;
}

void test_depth_cache_set(void **state)
{
    int ret = init_depth_cache(cache_timeout);
    assert_int_equal(ret, 0);
    
    const char *market = "CETUSDT";
    const char *interval = "0.1";
    const int limit = 50;

    json_t *data = get_json(market, interval, limit);
    assert_int_equal(0, depth_cache_set(market, interval, limit, data));
    
    struct depth_cache_val *val = depth_cache_get(market, interval, limit);
    assert_true(val != NULL);
    assert_true(cache_not_expired(val));
    assert_true(last_limit_not_expired(val));
    assert_int_equal(limit, val->limit);
    assert_int_equal(0, val->second_limit);
    assert_true(is_json_equal(data, val->data));
    json_decref(data);

    const int limit1 = 60;
    data = get_json(market, interval, limit);
    assert_int_equal(0, depth_cache_set(market, interval, limit1, data));
    assert_true(is_json_equal(data, val->data));
    json_decref(data);

    val = depth_cache_get(market, interval, limit);
    assert_true(val != NULL);
    assert_true(cache_not_expired(val));
    assert_true(last_limit_not_expired(val));
    assert_int_equal(limit1, val->limit);
    assert_int_equal(limit, val->second_limit);

    fini_depth_cache();
}

void test_depth_cache_get(void **state)
{
    int ret = init_depth_cache(cache_timeout);
    assert_int_equal(ret, 0);

    const char *market = "CETUSDT";
    const char *interval = "0.1";
    const int limit = 50;
    json_t *data = get_json(market, interval, limit);
    depth_cache_set(market, interval, limit, data);
    json_decref(data);
    
    struct depth_cache_val *val = depth_cache_get(market, interval, limit);
    assert_true(depth_cache_get(market, interval, limit) != NULL);
    assert_true(depth_cache_get(market, interval, limit - 10) != NULL);
    assert_true(depth_cache_get(market, interval, limit + 10) == NULL);
    assert_true(depth_cache_get("GGG", interval, limit) == NULL);

    fini_depth_cache();
}

void test_depth_cache_get_update_limit(void **state)
{
    int ret = init_depth_cache(cache_timeout);
    assert_int_equal(ret, 0);

    const char *market = "CETUSDT";
    const char *interval = "0.1";
    const int limit = 50;
    json_t *data = get_json(market, interval, limit);
    depth_cache_set(market, interval, limit, data);
    json_decref(data);
    
    struct depth_cache_val *val = depth_cache_get(market, interval, limit);
    assert_int_equal(limit, depth_cache_get_update_limit(val, limit));

    const int limit1 = 40;
    assert_int_equal(limit, depth_cache_get_update_limit(val, limit1));
    val = depth_cache_get(market, interval, limit1);
    
    // val->limit(50)过期，val->second_limit为40，此时获取到的更新limit应该为40
    const int limit2 = 30;
    expire_limit(val);
    assert_int_equal(limit1, depth_cache_get_update_limit(val, limit2));
    assert_int_equal(limit1, val->limit);
    assert_int_equal(limit2, val->second_limit);
    
    // val->limit(40)过期，val->second_limit为30，此时获取到的更新limit应该为35
    const int limit3 = 35;
    expire_limit(val);
    assert_int_equal(limit3, depth_cache_get_update_limit(val, limit3));
    assert_int_equal(limit3, val->limit);
    assert_int_equal(limit2, val->second_limit);

    fini_depth_cache();    
}

int main()
{
    const UnitTest tests[] = {  
        unit_test(test_depth_cache_set),
        unit_test(test_depth_cache_get),
        unit_test(test_depth_cache_get_update_limit)
    }; 
    
    return run_tests(tests);  
}

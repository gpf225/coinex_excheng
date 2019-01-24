# ifndef _TEST_LP_DEPTH_H_
# define _TEST_LP_DEPTH_H_

# include "../../longpoll/lp_depth_sub.h"

# include <stdarg.h>
# include <stddef.h>
# include <setjmp.h>
# include <google/cmockery.h>

static nw_ses* new_ses() 
{
    struct nw_ses *ses = malloc(sizeof(nw_ses));
    memset(ses, 0, sizeof(nw_ses));
    return ses;
}

static struct depth_limit_val* get_depth_limit_list(dict_t *depth_item, const char *market, const char *interval) 
{
    struct depth_key key;
    depth_set_key(&key, market, interval, 0);

    dict_entry *entry = dict_find(depth_item, &key);
    if (entry == NULL) {
        return NULL;
    }

    return entry->val;
}

void test_depth_subscribe(void **state)
{
    int ret = init_depth_sub();
    assert_int_equal(ret, 0);

    struct nw_ses *ses = new_ses(); 
    dict_t *depth_sub = depth_get_sub();
    dict_t *depth_item = depth_get_item();
    assert_int_equal(dict_size(depth_sub), 0);
    assert_int_equal(dict_size(depth_item), 0);

    assert_int_equal(depth_subscribe(ses, "CETBCH", "0.01", 10), 0);      // 订阅成功返回0
    assert_int_equal(depth_subscribe(ses, "CETBCH", "0.01", 10), 0);      // 重复订阅返回0,但是不会增加订阅数量
    assert_int_equal(dict_size(depth_sub), 1);
    assert_int_equal(dict_size(depth_item), 1);

    depth_subscribe(ses, "CETBTC", "0.01", 10);
    depth_subscribe(ses, "CETBTC", "0.01", 20);
    depth_subscribe(ses, "CETBTC", "0.01", 30);
    depth_subscribe(ses, "CETBTC", "0.01", 40);

    assert_int_equal(dict_size(depth_sub), 5);
    assert_int_equal(dict_size(depth_item), 2);  // depth_item以(market, interval)为唯一标志，不同的limit都对应同一组(market, interval)

    free(ses);
    fini_depth_sub();
}

void test_depth_unsubscribe(void **state)
{
    int ret = init_depth_sub();
    assert_int_equal(ret, 0);
    
    dict_t *depth_sub = depth_get_sub();
    dict_t *depth_item = depth_get_item();
    assert_int_equal(dict_size(depth_sub), 0);
    assert_int_equal(dict_size(depth_item), 0);

    struct nw_ses *ses1 = new_ses();
    struct nw_ses *ses2 = new_ses();

    depth_subscribe(ses1, "CETBCH", "0.01", 10);
    depth_subscribe(ses1, "CETBCH", "0.01", 20);
    depth_subscribe(ses1, "CETBCH", "0.01", 30);

    depth_subscribe(ses2, "CETBTC", "0.01", 10);
    depth_subscribe(ses2, "CETBTC", "0.01", 20);
    depth_subscribe(ses2, "CETBTC", "0.01", 30);

    assert_int_equal(dict_size(depth_sub), 6);
    assert_int_equal(dict_size(depth_item), 2);


    assert_int_equal(depth_unsubscribe(ses1, "CETBCH", "0.01", 10), 0);
    assert_int_equal(depth_unsubscribe(ses1, "CETBCH", "0.01", 10), 0);  // 重复取消返回0
    assert_int_equal(dict_size(depth_sub), 5);
    assert_int_equal(dict_size(depth_item), 2);

    depth_unsubscribe(ses1, "CETBCH", "0.01", 20);
    depth_unsubscribe(ses1, "CETBCH", "0.01", 30);

    assert_int_equal(dict_size(depth_sub), 3);
    assert_int_equal(dict_size(depth_item), 1);
    assert_true(get_depth_limit_list(depth_item, "CETBCH", "0.01") == NULL); 

    assert_int_equal(depth_unsubscribe(ses1, "CETBTC", "0.01", 10), 0);  // 取消不存在的订阅返回0

    struct depth_limit_val *list = get_depth_limit_list(depth_item, "CETBTC", "0.01");
    assert_int_equal(list->size, 3);
    assert_int_equal(list->max, 30);

    depth_unsubscribe(ses2, "CETBTC", "0.01", 30);
    assert_int_equal(list->size, 2);
    assert_int_equal(list->max, 20);
  
    depth_unsubscribe(ses2, "CETBTC", "0.01", 10);  
    depth_unsubscribe(ses2, "CETBTC", "0.01", 20);
    assert_int_equal(dict_size(depth_sub), 0);
    assert_int_equal(dict_size(depth_item), 0);
    assert_true(get_depth_limit_list(depth_item, "CETBTC", "0.01") == NULL);

    free(ses1);
    free(ses2);
    fini_depth_sub();
}

void test_depth_unsubscribe_all(void **state)
{
    int ret = init_depth_sub();
    assert_int_equal(ret, 0);
    
    dict_t *depth_sub = depth_get_sub();
    dict_t *depth_item = depth_get_item();
    assert_int_equal(dict_size(depth_sub), 0);
    assert_int_equal(dict_size(depth_item), 0);

    struct nw_ses *ses1 = new_ses();
    struct nw_ses *ses2 = new_ses();

    depth_subscribe(ses1, "CETBTC", "0.01", 10);
    depth_subscribe(ses1, "CETBTC", "0.01", 20);

    depth_subscribe(ses2, "CETBTC", "0.01", 10);
    depth_subscribe(ses2, "CETBTC", "0.01", 20);
    depth_subscribe(ses2, "CETBCH", "0.01", 10);
    depth_subscribe(ses2, "CETBCH", "0.01", 20);
    assert_int_equal(dict_size(depth_sub), 4);
    assert_int_equal(dict_size(depth_item), 2);

    assert_int_equal(depth_unsubscribe_all(ses1), 2);
    assert_int_equal(depth_unsubscribe_all(ses1), 0);
    assert_int_equal(dict_size(depth_sub), 4);
    assert_int_equal(dict_size(depth_item), 2);

    assert_int_equal(depth_unsubscribe_all(ses2), 4);
    assert_int_equal(dict_size(depth_sub), 0);
    assert_int_equal(dict_size(depth_item), 0);

    free(ses1);
    free(ses2);
    fini_depth_sub();
}

int main()
{
    const UnitTest tests[] = {  
        unit_test(test_depth_subscribe),
        unit_test(test_depth_unsubscribe),
        unit_test(test_depth_unsubscribe_all)
    }; 
    
    return run_tests(tests);  
}

# endif
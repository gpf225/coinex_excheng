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

void test_depth_subscribe_press(void **state)
{
    int ret = init_depth_sub();
    assert_int_equal(ret, 0);

    dict_t *depth_sub = depth_get_sub();
    dict_t *depth_item = depth_get_item();
    assert_int_equal(dict_size(depth_sub), 0);
    assert_int_equal(dict_size(depth_item), 0);
   
    const int market_total = 100;
    char *markets[100];
    for (int i = 0; i < market_total; ++i) {
        char buf[32] = {0};
        snprintf(buf, sizeof(buf), "market_%d", i);
        markets[i] = strdup(buf);
    }
    
    const int ses_total = 100;
    nw_ses *seses[100];
    for (int i = 0; i < ses_total; ++i) {
        seses[i] = new_ses();
        printf("ses[%d]:%p\n", i, seses[i]);
    }
     
    const char *interval = "0";
    const int limit = 50;
    for (int i = 0; i < ses_total; ++i) {
        for (int j = 0; j < market_total; ++j) {
            printf("ses:%p i:%d j:%d\n", seses[i], i, j);
            depth_subscribe(seses[i], markets[j], interval, limit);
        }
    }

    printf("depth_sub size:%u\n", dict_size(depth_sub));
    printf("depth_item size:%u\n", dict_size(depth_item));

    depth_unsubscribe_all(seses[2]);
    depth_unsubscribe_all(seses[5]);

    printf("depth_sub size:%u\n", dict_size(depth_sub));
    printf("depth_item size:%u\n", dict_size(depth_item));
    
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(depth_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *val = entry->val;
        dict_t *sessions = val->sessions;
        struct depth_key *key = entry->key;
        printf("market:%s sessions:%u\n", key->market, dict_size(sessions));
    }
    dict_release_iterator(iter);
     
    for (int i = 0; i < ses_total; ++i) {
        free(seses[i]);
    }

    for (int i = 0; i < market_total; ++i) {
        free(markets[i]);
    }
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

    dict_t *dict_depth_sub = depth_get_sub();
    dict_entry *entry = NULL;
    dict_iterator *iter = dict_get_iterator(dict_depth_sub);
    while ((entry = dict_next(iter)) != NULL) {
        struct depth_val *val = entry->val;
        dict_t *sessions = val->sessions;
        assert_int_equal(1, dict_size(sessions));
        assert_true(dict_find(sessions, ses2) != NULL);
        assert_true(dict_find(sessions, ses1) == NULL);
    }
    dict_release_iterator(iter);

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
        unit_test(test_depth_unsubscribe_all),
        unit_test(test_depth_subscribe_press)
    }; 
    
    return run_tests(tests);  
    
    //test_depth_subscribe_press(NULL);
    //return 0;
}

# endif
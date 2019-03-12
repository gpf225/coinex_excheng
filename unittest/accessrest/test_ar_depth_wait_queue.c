# include "../../accessrest/ar_depth_wait_queue.h"

# include <stdarg.h>
# include <stddef.h>
# include <setjmp.h>
# include <google/cmockery.h>

# include "ut_misc.h"

static nw_ses* new_ses() 
{
    struct nw_ses *ses = malloc(sizeof(nw_ses));
    memset(ses, 0, sizeof(nw_ses));
    return ses;
}

void test_depth_wait_queue_add(void **state)
{
    int ret = init_depth_wait_queue();
    assert_int_equal(ret, 0);

    struct nw_ses *ses1 = new_ses();
    ret = depth_wait_queue_add("CETBTC", "0", 50, ses1, 1);
    assert_int_equal(ret, 1);
    ret = depth_wait_queue_add("CETBTC", "0", 50, ses1, 1);
    assert_int_equal(ret, 0);
    ret = depth_wait_queue_add("CETBTC", "0", 50, ses1, 2);
    assert_int_equal(ret, 1);
    ret = depth_wait_queue_add("CETBTC", "0", 100, ses1, 2);
    assert_int_equal(ret, 0);

    struct nw_ses *ses2 = new_ses();
    ret = depth_wait_queue_add("CETBTC", "0", 50, ses2, 1);
    assert_int_equal(ret, 1);
    ret = depth_wait_queue_add("CETBTC", "0", 50, ses2, 2);
    assert_int_equal(ret, 1);

    free(ses1);
    free(ses2);
    fini_depth_wait_queue();
}

void test_depth_wait_queue_remove(void **state)
{
    int ret = init_depth_wait_queue();
    assert_int_equal(ret, 0);

    struct nw_ses *ses1 = new_ses();
    struct nw_ses *ses2 = new_ses();

    depth_wait_queue_add("CETBTC", "0", 50, ses1, 1);
    depth_wait_queue_add("CETBTC", "0", 50, ses1, 2);
    depth_wait_queue_add("CETBTC", "0", 50, ses1, 3);
    depth_wait_queue_add("CETBTC", "0", 50, ses1, 4);

    depth_wait_queue_add("CETBTC", "0", 50, ses2, 5);
    depth_wait_queue_add("CETBTC", "0", 50, ses2, 6);

    struct depth_wait_val *wait_val = depth_wait_queue_get("CETBTC", "0");
    dict_t *wait_session = wait_val->dict_wait_session;
    dict_entry *entry = dict_find(wait_session, ses1);
    list_t *wait_list = entry->val;

    assert_int_equal(dict_size(wait_session), 2);
    assert_int_equal(list_len(wait_list), 4);
    
    ret = depth_wait_queue_remove("CETBTC", "0", 50, ses1, 1);
    assert_int_equal(list_len(wait_list), 3);
    ret = depth_wait_queue_remove("CETBTC", "0", 50, ses1, 2);
    assert_int_equal(list_len(wait_list), 2);
    ret = depth_wait_queue_remove("CETBTC", "0", 50, ses1, 3);
    assert_int_equal(list_len(wait_list), 1);
    ret = depth_wait_queue_remove("CETBTC", "0", 50, ses1, 4);
    assert_int_equal(list_len(wait_list), 0);
    assert_int_equal(dict_size(wait_session), 1);

    free(ses1);
    free(ses2);
    fini_depth_wait_queue();
}

void test_depth_wait_queue_remove_all(void **state)
{
    int ret = init_depth_wait_queue();
    assert_int_equal(ret, 0);

    struct nw_ses *ses1 = new_ses();
    struct nw_ses *ses2 = new_ses();

    depth_wait_queue_add("CETBTC1", "0", 50, ses1, 1);
    depth_wait_queue_add("CETBTC1", "0", 50, ses1, 2);
    depth_wait_queue_add("CETBTC2", "0", 50, ses1, 3);
    depth_wait_queue_add("CETBTC2", "0", 50, ses1, 4);

    depth_wait_queue_add("CETBTC1", "0", 50, ses2, 5);
    depth_wait_queue_add("CETBTC2", "0", 50, ses2, 6);

    struct depth_wait_val *wait_val = depth_wait_queue_get("CETBTC1", "0");
    dict_t *wait_session = wait_val->dict_wait_session;
    dict_entry *entry = dict_find(wait_session, ses1);
    list_t *wait_list = entry->val;

    assert_int_equal(dict_size(wait_session), 2);
    assert_int_equal(list_len(wait_list), 2);
    
    int count = depth_wait_queue_remove_all(ses1);
    assert_int_equal(count, 4);
    assert_int_equal(dict_size(wait_session), 1);

    free(ses1);
    free(ses2);
    fini_depth_wait_queue();
}

int run_test1(void)
{
    const UnitTest tests[] = {  
        unit_test(test_depth_wait_queue_add),
        unit_test(test_depth_wait_queue_remove),
        unit_test(test_depth_wait_queue_remove_all)
    }; 
    
    return run_tests(tests);  
}
int main()
{
    return run_test1(); 

    //test_depth_wait_queue_add(NULL);
    //return 0;   
}

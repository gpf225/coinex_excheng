
#include "AccountTransfer.h"
#include "FieldSelector.h"
#include "Utils.h"

int AccountTransfer::init() {
    balance_writer_ = make_shared<BalanceWriter>(ctx_);
    return 0;
}

int AccountTransfer::handle() {
    string sql = "select * from core_account";
    int ret = mysql_real_query(ctx_.src_db_conn_, sql.c_str(), sql.length());
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql.c_str(), mysql_errno(ctx_.src_db_conn_), mysql_error(ctx_.src_db_conn_));
        return -1;
    }

    MYSQL_RES *result = mysql_store_result(ctx_.src_db_conn_);
    stat_->total_ = mysql_num_rows(result);
    FieldSelector selector(result);

    using Account=src_ns::Account;
    vector<shared_ptr<Account>> accounts;

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)))  {
        shared_ptr<Account> account(new Account());

        account->appl_id = strtoul(row[selector["appl_id"]],nullptr,0);
        account->user_id = strtoul(row[selector["user_id"]],nullptr,0);
        account->currency_id = strtoul(row[selector["currency_id"]],nullptr,0);
        account->total_money = row[selector["total_money"]];
        account->order_frozen_money = row[selector["order_frozen_money"]];
        account->update_time = strtoull(row[selector["update_time"]],nullptr,0);
        account->is_forbid = strtoul(row[selector["is_forbid"]],nullptr,0);

        accounts.push_back(account);
    }

    for (auto account : accounts) {
        int ret = convert(*account.get());
        if (ret) {
            stat_->fail_++;
            log_error("convert order fail,user_id:%d,asset:%d",account->user_id,account->currency_id);
        }
        else
            stat_->success_++;
    }

    ret = save();

    return ret;
}


int AccountTransfer::convert(src_ns::Account &src_account) {
    balance_t *balance = new balance_t();
    balance->user_id = src_account.user_id;
    balance->account = src_account.appl_id;
    const Coin *coin = ctx_.get_coin(src_account.currency_id);
    if (coin==nullptr) {
        delete balance;
        return -1;
    }
    balance->asset = strdup(coin->name_.c_str());
    balance->update_time = src_account.update_time/1000000;

    mpd_t *total = decimal(src_account.total_money.c_str(),0);
    mpd_t *frozen = decimal(src_account.order_frozen_money.c_str(),0);
    balance->t = 1;
    mpd_t *avail = mpd_qncopy(total);
    mpd_sub(avail,avail,frozen,&mpd_ctx);
    balance->balance = avail;
    balances_.push_back(shared_ptr<balance_t>(balance));

    if (mpd_cmp(frozen,mpd_zero,&mpd_ctx)!=0) {
        balance_t *b = new balance_t();
        b->user_id = src_account.user_id;
        b->account = src_account.appl_id;
        b->asset = strdup(coin->name_.c_str());
        b->update_time = src_account.update_time/1000000;
        b->t = 2;
        b->balance = mpd_qncopy(frozen);
        balances_.push_back(shared_ptr<balance_t>(b));
    }
    mpd_del(total);
    mpd_del(frozen);

    return 0;
}

int AccountTransfer::save() {
    ctx_.timestamp_ =  time(NULL);
    int ret = balance_writer_->generate_table();
    if (ret)
        return -1;

    CLargeStringArray data;
    for (auto balance : balances_) {
        string value = balance_writer_->generate_value_list(*balance.get());
        data.Add(Utils::dup_string(value));
    }
    ret = balance_writer_->batch_insert(data,ctx_.settings_.row_limit);
    if (ret)
        return -1;

    return 0;
}

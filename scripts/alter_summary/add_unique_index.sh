#!/bin/bash

MYSQL_SUMMARY_HOST="192.168.0.95"
MYSQL_SUMMARY_USER="root"
MYSQL_SUMMARY_PASS="shit"
MYSQL_SUMMARY_DB="trade_summary"

function alter_user_trade_summary() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'user_trade_summary_%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table $table_name add unique index idx_user_market_date (user_id, market, trade_date);"
    done
}

function alter_user_fee_summary() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'user_fee_summary_%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table $table_name add unique index idx_user_market_asset_date (user_id, market, asset, trade_date);"
    done
}

function alert_client_trade_summary() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'client_trade_summary_%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table $table_name drop index idx_client_market_user_date;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table $table_name add unique index idx_client_market_user_date(client_id, market, user_id, trade_date);"
    done
}

function alert_client_fee_summary() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'client_fee_summary_%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table $table_name add unique index idx_user_client_market_asset_date(user_id, client_id, market, asset, trade_date);"
    done
}

function alter_coin_trade_summary() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table coin_trade_summary drop index idx_market_date;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table coin_trade_summary add unique index idx_market_date(market, trade_date);"
}

function alter_dump_history() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table dump_history add deals_offset BIGINT NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table dump_history add orders_offset BIGINT NOT NULL default 0;"
}

alter_user_trade_summary
alter_user_fee_summary
alert_client_trade_summary
alert_client_fee_summary
alter_coin_trade_summary
alter_dump_history

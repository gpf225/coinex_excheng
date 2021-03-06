#!/bin/bash

MYSQL_SUMMARY_HOST="127.0.0.1"
MYSQL_SUMMARY_USER="root"
MYSQL_SUMMARY_PASS="shit"
MYSQL_SUMMARY_DB="trade_summary"

function alter_user_summary_example() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_trade_summary_example add taker_amount DECIMAL(40,20) NOT NULL;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_trade_summary_example add taker_volume DECIMAL(40,20) NOT NULL;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_trade_summary_example add maker_amount DECIMAL(40,20) NOT NULL;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_trade_summary_example add maker_volume DECIMAL(40,20) NOT NULL;"
}

function alter_table() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add taker_amount DECIMAL(40,20) NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add taker_volume DECIMAL(40,20) NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add maker_amount DECIMAL(40,20) NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add maker_volume DECIMAL(40,20) NOT NULL default 0;"
}

function alter_user_summary_history() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'user_trade_summary_2%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    alter_table $table_name
    done
}

alter_user_summary_example
alter_user_summary_history


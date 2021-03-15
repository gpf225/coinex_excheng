#!/bin/bash

MYSQL_HOST = "192.168.0.95"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_summary"

'''
MYSQL_HOST = "coinexlog.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_USER = "coinex"
MYSQL_PASS = "hp1sXMJftZWPO5bQ2snu"
MYSQL_DB = "trade_summary"
MYSQL_PORT = 3306
'''

function alter_user_detail_example() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_detail_example add taker_amount DECIMAL(40,20) NOT NULL;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_detail_example add maker_amount DECIMAL(40,20) NOT NULL;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_detail_example add taker_volume DECIMAL(40,20) NOT NULL;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table user_detail_example add maker_volume DECIMAL(40,20) NOT NULL;"
}

function alter_user_detail_tables() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add taker_amount DECIMAL(40,20) NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add maker_amount DECIMAL(40,20) NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add taker_volume DECIMAL(40,20) NOT NULL default 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table "$1" add maker_volume DECIMAL(40,20) NOT NULL default 0;"
}

function alter_user_detail() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'user_detail_2%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    alter_user_detail_tables $table_name
    done
}

alter_user_detail_example
alter_user_detail

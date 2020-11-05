#!/bin/bash

MYSQL_SUMMARY_HOST="192.168.0.95"
MYSQL_SUMMARY_USER="root"
MYSQL_SUMMARY_PASS="shit"
MYSQL_SUMMARY_DB="kline_coinex"

function add_kline_index() {
    table_list=`mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} -D${MYSQL_SUMMARY_DB} -N -e "show tables like 'kline_history_%'"`

    for table_name in $table_list                                                                                                      
    do
    echo "alter table $table_name"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table $table_name add index idx_type_timestamp(t, timestamp);"
    done
}


add_kline_index



#!/bin/bash

#MYSQL_SUMMARY_HOST="192.168.0.95"
#MYSQL_SUMMARY_USER="root"
#MYSQL_SUMMARY_PASS="shit"
#MYSQL_SUMMARY_DB="trade_log"

MYSQL_SUMMARY_HOST="coinexlog.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_SUMMARY_USER="coinex"
MYSQL_SUMMARY_PASS="hp1sXMJftZWPO5bQ2snu"
MYSQL_SUMMARY_DB="trade_log"

function alter_slice_order_example() {
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table slice_order_example ADD money_fee DECIMAL(40,24) NOT NULL DEFAULT 0;"
    mysql -h${MYSQL_SUMMARY_HOST} -u${MYSQL_SUMMARY_USER} -p${MYSQL_SUMMARY_PASS} ${MYSQL_SUMMARY_DB} -e "alter table slice_order_example ADD stock_fee DECIMAL(40,24) NOT NULL DEFAULT 0;"
}

alter_slice_order_example

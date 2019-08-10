#!/bin/bash

MYSQL_LOG_HOST="127.0.0.1"
MYSQL_LOG_USER="root"
MYSQL_LOG_PASS="shit"
MYSQL_LOG_DB="trade_log"

MYSQL_HISTORY_HOST=("127.0.0.1" "127.0.0.1" "127.0.0.1" "127.0.0.1" "127.0.0.1")
MYSQL_HISTORY_USER=("root" "root" "root" "root" "root")
MYSQL_HISTORY_PASS=("shit" "shit" "shit" "shit" "shit")
MYSQL_HISTORY_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)

LEN=${#MYSQL_HISTORY_DB[@]}

function alter_log_table() {
	#slice_balance 修改精度20改成24
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_balance_example modify balance DECIMAL(40,24) NOT NULL;"

	#slice_order 修改精度20改成24
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_order_example modify frozen DECIMAL(40,24) NOT NULL;"
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_order_example modify deal_fee DECIMAL(40,24) NOT NULL;"
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_order_example modify asset_fee DECIMAL(40,24) NOT NULL;"

	#slice_order 增加option
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_order_example add \`option\` INT UNSIGNED NOT NULL default 0 AFTER account;"

	#slice_stop 增加option
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_stop_example add \`option\` INT UNSIGNED NOT NULL default 0 AFTER account;"

    #index_log 修改精度为12
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table indexlog_example modify price DECIMAL(40,12);"
}

function alter_history_table() {
	for((x = 0; x < $LEN; x++))
	do
		for i in `seq 0 99`
	    do
	    	echo "alter_history database:$x  seq:$i"
			#order_history
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table order_history_$i add \`option\` INT UNSIGNED NOT NULL default 0 COMMENT '按位操作' AFTER account;"

			#stop_history
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table stop_history_$i add \`option\` INT UNSIGNED NOT NULL default 0 COMMENT '按位操作' AFTER account;"
	    done
    done
}

alter_log_table
alter_history_table


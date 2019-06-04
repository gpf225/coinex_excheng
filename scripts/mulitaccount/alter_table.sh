#!/bin/bash

MYSQL_LOG_HOST="127.0.0.1"
MYSQL_LOG_USER="root"
MYSQL_LOG_PASS="shit"
MYSQL_LOG_DB="trade_log"

MYSQL_HISTORY_HOST=("127.0.0.1" "127.0.0.1" "127.0.0.1" "127.0.0.1" "127.0.0.1")
MYSQL_HISTORY_USER=("root" "root" "root" "root" "root")
MYSQL_HISTORY_PASS=("shit" "shit" "shit" "shit" "shit")
MYSQL_HISTORY_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)

slice_time=1558576800
LEN=${#MYSQL_HISTORY_DB[@]}

function alter_log_table_add_account() {
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_balance_example add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_balance_$slice_time add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"

    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_update_example add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_update_$slice_time add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"

    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_order_example add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_order_$slice_time add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"

    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_stop_example add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"
    mysql -h${MYSQL_LOG_HOST} -u${MYSQL_LOG_USER} -p${MYSQL_LOG_PASS} ${MYSQL_LOG_DB} -e "alter table slice_stop_$slice_time add account INT UNSIGNED NOT NULL default 0 AFTER user_id;"

}

function alter_history_table_add_index_account() {
	for((x = 0; x < $LEN; x++))
	do
		for i in `seq 0 99`
	    do
	    	echo "add_index_account database:$x  seq:$i"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i add account INT UNSIGNED NOT NULL default 0 COMMENT '用户账户ID' AFTER user_id;"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table order_history_$i add account INT UNSIGNED NOT NULL default 0 COMMENT '用户账户ID' AFTER user_id;"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table stop_history_$i add account INT UNSIGNED NOT NULL default 0 COMMENT '用户账户ID' AFTER user_id;"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table user_deal_history_$i add account INT UNSIGNED NOT NULL default 0 COMMENT '用户账户ID' AFTER user_id;"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table user_deal_history_$i add deal_account INT UNSIGNED NOT NULL default 0 COMMENT '对手账户ID' AFTER deal_user_id;"

	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i add INDEX idx_user_account_time (user_id, account, time);"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i add INDEX idx_user_account_business_time (user_id, account, business, time);"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i add INDEX idx_user_account_asset_business_time (user_id, account, asset, business, time);"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table order_history_$i add INDEX idx_user_account_market_side_time (user_id, account, market, side, create_time);"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table stop_history_$i add INDEX idx_user_account_market_side_time (user_id, account, market, side, create_time);"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table user_deal_history_$i add INDEX idx_user_account_market_side_time (user_id, account, market, side, time);"
	    done
    done
}

function alter_history_table_drop_index() {
	for((x = 0; x < $LEN; x++))
	do
		for i in `seq 0 99`
	    do
	    	echo "drop_index database:$x  seq:$i"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i drop INDEX idx_user_time;"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i drop INDEX idx_user_business_time;"
	    	mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table balance_history_$i drop INDEX idx_user_asset_business_time;"
	    done
    done
}

alter_history_table_add_index_account



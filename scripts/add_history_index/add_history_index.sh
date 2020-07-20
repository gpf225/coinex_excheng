#!/bin/bash


#MYSQL_HISTORY_HOST=("coinextradehistory0.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory1.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory2.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory3.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory4.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com")
#MYSQL_HISTORY_USER=("root" "root" "root" "root" "root")
#MYSQL_HISTORY_PASS=("6jh7QCaj4gX8QVx4T7j6" "7BG5CWFvPAOOdx99Gytn" "lsD9idvDE0b26W6V474M" "60yQcHSNB76PQtl7HvQA" "Hs7NMTIdG58Zk7sP68vD")
#MYSQL_HISTORY_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)

MYSQL_HISTORY_HOST=("127.0.0.1" "127.0.0.1" "127.0.0.1" "127.0.0.1" "127.0.0.1")
MYSQL_HISTORY_USER=("root" "root" "root" "root" "root")
MYSQL_HISTORY_PASS=("shit" "shit" "shit" "shit" "shit")
MYSQL_HISTORY_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)


LEN=${#MYSQL_HISTORY_DB[@]}

function alter_history_table_add_index() {
    for((x = 0; x < $LEN; x++))
    do
        for i in `seq 0 99`
        do
            echo "add_index database:$x  seq:$i"
            mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table order_history_$i add INDEX idx_user_account_market_time(user_id, account, market, create_time);"
            mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table stop_history_$i add INDEX idx_user_account_market_time(user_id, account, market, create_time);"
            mysql -h${MYSQL_HISTORY_HOST[x]} -u${MYSQL_HISTORY_USER[x]} -p${MYSQL_HISTORY_PASS[x]} ${MYSQL_HISTORY_DB[x]} -e "alter table user_deal_history_$i add INDEX idx_user_account_market_time(user_id, account, market, time);"

        done
    done
}

alter_history_table_add_index



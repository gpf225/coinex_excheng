#!/bin/bash

#MYSQL_HOST=("localhost" "localhost" "localhost" "localhost" "localhost")
#MYSQL_USER=("root" "root" "root" "root" "root")
#MYSQL_PASS=("shit" "shit" "shit" "shit" "shit")
#MYSQL_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)

MYSQL_HOST=("coinextradehistory0.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory1.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory2.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory3.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com" "coinextradehistory4.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com")
MYSQL_USER=("coinex" "coinex" "coinex" "coinex" "coinex")
MYSQL_PASS=("6jh7QCaj4gX8QVx4T7j6" "7BG5CWFvPAOOdx99Gytn" "lsD9idvDE0b26W6V474M" "60yQcHSNB76PQtl7HvQA" "Hs7NMTIdG58Zk7sP68vD")
MYSQL_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)

LEN=${#MYSQL_DB[@]}

function alter_history_tables() {
    for i in `seq 0 99`
    do
        echo "$1 $2 $3 $4 create table balance_history_$i"
        mysql -h$1 -u$2 -p$3 $4 -e "alter table order_history_$i ADD money_fee DECIMAL(40,20) NOT NULL DEFAULT 0;"
        mysql -h$1 -u$2 -p$3 $4 -e "alter table order_history_$i ADD stock_fee DECIMAL(40,20) NOT NULL DEFAULT 0;"
    done
}

alter_history_tables

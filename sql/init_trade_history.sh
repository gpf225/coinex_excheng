#!/bin/bash

MYSQL_HOST=("localhost" "localhost" "localhost" "localhost" "localhost")
MYSQL_USER=("root" "root" "root" "root" "root")
MYSQL_PASS=("12345678" "12345678" "12345678" "12345678" "12345678")
MYSQL_DB=(trade_history_0 trade_history_1 trade_history_2 trade_history_3 trade_history_4)

LEN=${#MYSQL_DB[@]}

function create_all_database() {
    for((i = 0; i < $LEN; i++))
    do
        mysql -h${MYSQL_HOST[i]} -u${MYSQL_USER[i]} -p${MYSQL_PASS[i]} -e "create database ${MYSQL_DB[i]}"
    done
}

function drop_all_database() {
    for((i = 0; i < $LEN; i++))
    do
        mysql -h${MYSQL_HOST[i]} -u${MYSQL_USER[i]} -p${MYSQL_PASS[i]} -e "drop database ${MYSQL_DB[i]}"
    done
}

function create_balance_tables() {
    for i in `seq 0 99`
    do
        echo "$1 $2 $3 $4 create table balance_history_$i"
        mysql -h$1 -u$2 -p$3 $4 -e "CREATE TABLE balance_history_$i LIKE balance_history_example;"
    done
}

function create_order_tables() {
    for i in `seq 0 99`
    do
        echo "$1 $2 $3 $4 create table order_history_$i"
        mysql -h$1 -u$2 -p$3 $4 -e "CREATE TABLE order_history_$i LIKE order_history_example;"
    done
}

function create_stop_tables() {
    for i in `seq 0 99`
    do
        echo "$1 $2 $3 $4 create table stop_history_$i"
        mysql -h$1 -u$2 -p$3 $4 -e "CREATE TABLE stop_history_$i LIKE stop_history_example;"
    done 
}

function create_deal_tables() {
    for i in `seq 0 99`
    do
        echo "$1 $2 $3 $4 create table user_deal_history_$i"
        mysql -h$1 -u$2 -p$3 $4 -e "CREATE TABLE user_deal_history_$i LIKE user_deal_history_example;"
    done  
}

function create_example_tables() {
    for((i = 0; i < $LEN; i++))
    do
        mysql -h${MYSQL_HOST[i]} -u${MYSQL_USER[i]} -p${MYSQL_PASS[i]} ${MYSQL_DB[i]} -e "source create_trade_history.sql;"
    done
}

function create_all_tables() {
    for((j = 0; j < $LEN; j++))
    do
        create_balance_tables ${MYSQL_HOST[j]} ${MYSQL_USER[j]} ${MYSQL_PASS[j]} ${MYSQL_DB[j]}
        create_order_tables   ${MYSQL_HOST[j]} ${MYSQL_USER[j]} ${MYSQL_PASS[j]} ${MYSQL_DB[j]}
        create_stop_tables    ${MYSQL_HOST[j]} ${MYSQL_USER[j]} ${MYSQL_PASS[j]} ${MYSQL_DB[j]}
        create_deal_tables    ${MYSQL_HOST[j]} ${MYSQL_USER[j]} ${MYSQL_PASS[j]} ${MYSQL_DB[j]}
    done
}

if [[ $1 = "drop_20190331_coinex_ensure" ]];
then
    echo "---drop all databases---"
    drop_all_database
elif [[ $1 = "create" ]]; 
then   
    echo "---create all databases---"
    create_all_database
    create_example_tables
    create_all_tables
else
    echo "usage: bash init_trade_history.sh create"
    echo "       bash init_trade_history.sh drop"
    echo ""
    echo "create: create all databases, what you need will be created by this command"
    echo "drop:   drop all databases created in command create, if you want to drop database, please see this script and copy the drop option"
fi


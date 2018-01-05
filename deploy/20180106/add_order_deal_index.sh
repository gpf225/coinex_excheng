#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="shit"
MYSQL_DB="trade_history"

for i in `seq 0 99`
do
    echo "order_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "alter table order_history_$i add index idx_user_time (user_id, create_time);"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "alter table order_history_$i add index idx_user_side_time (user_id, side, create_time);"
done

for i in `seq 0 99`
do
    echo "user_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "alter table user_deal_history_$i add index idx_user_time (user_id, time);"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "alter table user_deal_history_$i add index idx_user_side_time (user_id, side, time);"
done

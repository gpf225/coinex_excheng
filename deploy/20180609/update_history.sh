#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="shit"
MYSQL_DB="trade_history"

# update order_history
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_example DROP fee_asset;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_example ADD fee_asset VARCHAR(30) NOT NULL AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_example ADD asset_fee DECIMAL(40,20) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table order_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_$i DROP fee_asset;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_$i ADD fee_asset VARCHAR(30) NOT NULL AFTER source;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_$i ADD asset_fee DECIMAL(40,20) NOT NULL DEFAULT 0;"
done

# update order_detail
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_example DROP fee_asset;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_example ADD fee_asset VARCHAR(30) NOT NULL default '' AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_example ADD asset_fee DECIMAL(40,20) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table order_detail_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_$i DROP fee_asset;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_$i ADD fee_asset VARCHAR(30) NOT NULL default '' AFTER source;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_$i ADD asset_fee DECIMAL(40,20) NOT NULL DEFAULT 0;"
done

# update order_deal_history
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_example ADD fee_asset VARCHAR(30) NOT NULL;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_example ADD deal_fee_asset VARCHAR(30) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table order_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_$i ADD fee_asset VARCHAR(30) NOT NULL;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_$i ADD deal_fee_asset VARCHAR(30) NOT NULL;"
done

# update user_deal_history
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_example ADD fee_asset VARCHAR(30);"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_example ADD deal_fee_asset VARCHAR(30) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table user_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_$i ADD fee_asset VARCHAR(30);"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_$i ADD deal_fee_asset VARCHAR(30) NOT NULL;"
done

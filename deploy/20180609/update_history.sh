#!/bin/bash

MYSQL_HOST="coinexdb.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_USER="coinex"
MYSQL_PASS="6g8nV76nYuMdFKSjA9hP"
MYSQL_DB="trade_history"

# update order_history
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_example ADD fee_asset VARCHAR(30) NOT NULL AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_example ADD fee_discount DECIMAL(40,4) NOT NULL AFTER fee_asset;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_example ADD asset_fee DECIMAL(40,20) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table order_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_$i ADD fee_asset VARCHAR(30) NOT NULL DEFAULT '' AFTER source;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_$i ADD fee_discount DECIMAL(40,4) NOT NULL DEFAULT 0 AFTER fee_asset;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_history_$i ADD asset_fee DECIMAL(40,20) NOT NULL DEFAULT 0;"
done

# update order_detail
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_example ADD fee_asset VARCHAR(30) NOT NULL AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_example ADD fee_discount DECIMAL(40,4) NOT NULL AFTER fee_asset;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_example ADD asset_fee DECIMAL(40,20) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table order_detail_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_$i ADD fee_asset VARCHAR(30) NOT NULL DEFAULT '' AFTER source;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_$i ADD fee_discount DECIMAL(40,4) NOT NULL DEFAULT 0 AFTER fee_asset;"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_detail_$i ADD asset_fee DECIMAL(40,20) NOT NULL DEFAULT 0;"
done

# update order_deal_history
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_example ADD fee_asset VARCHAR(30) NOT NULL;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_example ADD deal_fee_asset VARCHAR(30) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table order_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_$i ADD fee_asset VARCHAR(30) NOT NULL DEFAULT '';"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE order_deal_history_$i ADD deal_fee_asset VARCHAR(30) NOT NULL DEFAULT 0;"
done

# update user_deal_history
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_example ADD fee_asset VARCHAR(30) NOT NULL;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_example ADD deal_fee_asset VARCHAR(30) NOT NULL;"
for i in `seq 0 99`
do
    echo "update table user_deal_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_$i ADD fee_asset VARCHAR(30) NOT NULL DEFAULT '';"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE user_deal_history_$i ADD deal_fee_asset VARCHAR(30) NOT NULL DEFAULT 0;"
done

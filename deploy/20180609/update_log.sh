#!/bin/bash

MYSQL_HOST="coinexdb.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_USER="coinex"
MYSQL_PASS="6g8nV76nYuMdFKSjA9hP"
MYSQL_DB="trade_log"

LAST_SLICE="slice_order_1529863200"

echo "update slice_order_example"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE slice_order_example ADD fee_asset VARCHAR(30) NOT NULL AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE slice_order_example ADD fee_discount DECIMAL(40,4) NOT NULL AFTER fee_asset;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE slice_order_example ADD asset_fee DECIMAL(40,20) NOT NULL;"

mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE $LAST_SLICE ADD fee_asset VARCHAR(30) NOT NULL DEFAULT '' AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE $LAST_SLICE ADD fee_discount DECIMAL(40,4) NOT NULL DEFAULT 0 AFTER fee_asset;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE $LAST_SLICE ADD asset_fee DECIMAL(40,20) DEFAULT 0 NOT NULL;"

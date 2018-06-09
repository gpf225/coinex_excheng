#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="shit"
MYSQL_DB="trade_log"

echo "update slice_order_example"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE slice_order_example ADD fee_asset VARCHAR(30) NOT NULL AFTER source;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "ALTER TABLE slice_order_example ADD asset_fee DECIMAL(40,20) NOT NULL;"

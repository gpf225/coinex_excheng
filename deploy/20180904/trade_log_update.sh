#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="19862000"
MYSQL_DB="trade_log"

MODE_DEBUG="debug"
MODE_RELEASE="release"
MODE=$MODE_RELEASE

# price decimal(40, 8) => decimal(40, 12)
# deal_money decimal(40, 16) => decimal(40, 20)
SQL_UPDATE__SLICE_ORDER_EXAMPLE="ALTER TABLE slice_order_example MODIFY price decimal(40, 12) not null, MODIFY deal_money decimal(40, 20) not null;"

# price decimal(40, 8) => decimal(40, 12)
# stop_price decimal(40, 8) => decimal(40, 12)
SQL_UPDATE__SLICE_STOP_EXAMPLE="ALTER TABLE slice_stop_example MODIFY price decimal(40, 12) not null, MODIFY stop_price decimal(40, 12) not null;"

execute_update_sql() {
    UPDATE_SQL=$*
    if [ $MODE = $MODE_DEBUG ]; then
        echo "use debug mode, just print sql:" $UPDATE_SQL
    else
        echo "use release mode, sql:" $UPDATE_SQL
        mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "$UPDATE_SQL"
    fi
}

echo "update table slice_order_example"
execute_update_sql $SQL_UPDATE__SLICE_ORDER_EXAMPLE

echo "\n\nupdate table slice_stop_example"
execute_update_sql $SQL_UPDATE__SLICE_STOP_EXAMPLE
#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="19862000"
MYSQL_DB="trade_history"

MODE_DEBUG="debug"
MODE_RELEASE="release"
MODE=$MODE_RELEASE

UPDATE_SQL=""
MARK_TABLE_BALANCE_HISTORY_TABLES="balance_history"
MARK_TABLE_ORDER_HISTORY_TABLES="order_history"
MARK_TABLE_STOP_HISTORY_TABLES="stop_history"
MARK_TABLE_ORDER_DETAIL_HISTORY_TABLES="order_detail"
MARK_TABLE_ORDER_DEAL_HISTORY_TABLES="order_deal_history"
MARK_TABLE_USER_DEAL_HISTORY_TABLES="user_deal_history"

execute_update_sql() {
    if [ $MODE = $MODE_DEBUG ]; then
        echo "DEBUG:" $UPDATE_SQL
    else
        #echo "RELEASE:" $UPDATE_SQL
        mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "$UPDATE_SQL"
    fi
}

update_table() {
    TABLE=$1
    
    # update balance_history and balance_history_N tables
    # 'change' decimal(40, 8) => decimal(40, 20)
    if [[ $TABLE =~ $MARK_TABLE_BALANCE_HISTORY_TABLES ]]; then
        UPDATE_SQL="ALTER TABLE $TABLE MODIFY \`change\` decimal(40, 20) not null;"
    
    # update order_history and order_history_N 
    # 'price' decimal(40, 8) => decimal(40, 12)
    # 'deal_money' decimal(40, 16) => decimal(40, 20)
    elif [[ $TABLE =~ $MARK_TABLE_ORDER_HISTORY_TABLES ]]; then
        UPDATE_SQL="ALTER TABLE $TABLE MODIFY price decimal(40, 12) not null, MODIFY deal_money decimal(40, 20) not null;"
    
    # update stop_history and stop_history_N tables
    # 'price' decimal(40, 8) => decimal(40, 12)
    # 'stop_price' decimal(40, 8) => decimal(40, 12)
    elif [[ $TABLE =~ $MARK_TABLE_STOP_HISTORY_TABLES ]]; then
        UPDATE_SQL="ALTER TABLE $TABLE MODIFY price decimal(40, 12) not null, MODIFY stop_price decimal(40, 12) not null;"
    
    # update order_detail and order_detail_N 
    # 'price' decimal(40, 8) => decimal(40, 12)
    # 'deal_money' decimal(40, 16) => decimal(40, 20)
    elif [[ $TABLE =~ $MARK_TABLE_ORDER_DETAIL_HISTORY_TABLES ]]; then
        UPDATE_SQL="ALTER TABLE $TABLE MODIFY price decimal(40, 12) not null, MODIFY amount decimal(40, 12) not null, MODIFY deal_stock decimal(40, 12) not null;"
    
    # update order_deal_history and order_deal_history_N 
    # 'price' decimal(40, 8) => decimal(40, 12)
    # 'deal' decimal(40, 16) => decimal(40, 20)
    elif [[ $TABLE =~ $MARK_TABLE_ORDER_DEAL_HISTORY_TABLES ]]; then
        UPDATE_SQL="ALTER TABLE $TABLE MODIFY price decimal(40, 12) not null, MODIFY amount decimal(40, 12) not null;"
    
    # update user_deal_history and user_deal_history_N 
    # 'price' decimal(40, 8) => decimal(40, 12)
    # 'deal' decimal(40, 16) => decimal(40, 20)
    elif [[ $TABLE =~ $MARK_TABLE_USER_DEAL_HISTORY_TABLES ]]; then
        UPDATE_SQL="ALTER TABLE $TABLE MODIFY price decimal(40, 12) not null, MODIFY amount decimal(40, 12) not null;"
    
    else
        echo "table" $TABLE " not valid!"
        return 
    fi

    execute_update_sql
}

update_all_examples() {
    update_table "balance_history_example"
    update_table "order_history_example"
    update_table "stop_history_example"
    update_table "order_detail_example"
    update_table "order_deal_history_example"
    update_table "user_deal_history_example"
}


update_child_tables(){
    START=`date +%s`

    for i in `seq 0 99`
    do
        CHILD_TABLE="$1_$i"

        UPDATE_START=`date +%s`
        echo "update table:" $CHILD_TABLE ", start:" $UPDATE_START

        update_table $CHILD_TABLE

        UPDATE_END=`date +%s`
        echo "update table:" $CHILD_TABLE ", end:" $UPDATE_END ", costs:" $((END - START)) "seconds"
        echo ""
    done

    END=`date +%s`
    echo "update table:" $1 ", finished start:" `date -d @$START` ", end:" `date -d @$END` " costs:" $((END - START)) "seconds"
    echo ""
    echo ""
}

record_cost_time(){
    echo "update $1_s costs:" $2 " seconds"
    echo ""
}

SECOND_CONSTS_BALANCE_HISTORY=""
SECOND_CONST_ORDER_HISTPRY=""
SECOND_CONSTS_STOP_HISTORY=""
SECOND_CONSTS_ORDER_DETAIL=""
SECOND_CONSTS_ORDER_DEAL_HISTORY=""
SECOND_CONSTS_USER_DEAL_HISTORY=""

update_all_child_tables() {
    ALL_START=`date +%s`

    update_child_tables balance_history
    SECOND_CONSTS_BALANCE_HISTORY=$((END - START))

    update_child_tables order_history
    SECOND_CONST_ORDER_HISTPRY=$((END - START))

    update_child_tables stop_history  
    SECOND_CONSTS_STOP_HISTORY=$((END - START))

    update_child_tables order_detail
    SECOND_CONSTS_ORDER_DETAIL=$((END - START))

    update_child_tables order_deal_history
    SECOND_CONSTS_ORDER_DEAL_HISTORY=$((END - START))

    update_child_tables user_deal_history
    SECOND_CONSTS_USER_DEAL_HISTORY=$((END - START))
    
    ALL_END=`date +%s`
    record_cost_time balance_history $SECOND_CONSTS_BALANCE_HISTORY
    record_cost_time order_history $SECOND_CONST_ORDER_HISTPRY
    record_cost_time stop_history $SECOND_CONSTS_STOP_HISTORY
    record_cost_time order_detail $SECOND_CONSTS_ORDER_DETAIL
    record_cost_time order_deal_history $SECOND_CONSTS_ORDER_DEAL_HISTORY
    record_cost_time user_deal_history $SECOND_CONSTS_USER_DEAL_HISTORY

    echo ""
    echo ""
    echo "update start at:" `date -d @$ALL_START`  " end at:" `date -d @$ALL_END` " costs" $((ALL_END - ALL_START)) "seconds"
}


#update_all_examples

echo ""
echo ""
echo ""

update_all_child_tables






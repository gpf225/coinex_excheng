#encoding: utf-8
"""
    订单增加字段client_id: varchar(32);
"""
import os
import sys
import pymysql
import json
import traceback


DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASSWD = 'shit'
TRADE_LOG_DB = 'trade_log'
TRADE_HISTORY_FORMAT = 'trade_history_{}'


def check_column(db_conn, db_name, table_name, column_name):
    cursor = db_conn.cursor()
    sql_str = "select count(*) from columns where table_schema = '{}' and table_name = '{}' and column_name = '{}'".format(db_name, table_name, column_name)
    cursor.execute(sql_str)
    rows = cursor.fetchall()
    assert(len(rows) == 1)
    if rows[0][0] >= 1:
        #print("db_name:{}, table_name:{}, column_name:{} add failed:{}".format(db_name, table_name, column_name, rows[0][0]))
        return True
    cursor.close()
    return False


def modify_history_table(order_type):
    check_db_conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db='information_schema')
    for db_index in range(5):
        db_name = TRADE_HISTORY_FORMAT.format(db_index)
        conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=db_name)
        cursor = conn.cursor()
        table_name_format = '{}_history_{}'.format(order_type, '{}')
        table_sql_str_format = """alter table {} add client_id varchar(32) NOT NULL default "" COMMENT '用户自定义订单ID，默认为空';"""
        for table_id in range(100):
            table_name = table_name_format.format(table_id)
            if check_column(check_db_conn, db_name, table_name, 'client_id'):
                print("{} client id is exist.".format(table_name))
                continue
            sql_str = table_sql_str_format.format(table_name)
            cursor.execute(sql_str)
            conn.commit()

        table_name = table_name_format.format('example')
        if check_column(check_db_conn, db_name, table_name, 'client_id'):
            print("{} client id is exist.".format(table_name))
        else:
            sql_str = table_sql_str_format.format(table_name)
            cursor.execute(sql_str)
            conn.commit()
        cursor.close()
        conn.close()
    
    check_db_conn.close()


def modify_order_history_table():
    """修改订单历史表"""
    modify_history_table('order')


def modify_stop_order_history_table():
    """修改stop order历史表"""
    modify_history_table('stop')


def modify_slice_order_table():
    """修改slice order表"""
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=TRADE_LOG_DB)
    check_db_conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db='information_schema')
    cursor = conn.cursor()
    sql_str_format = """alter table {} add client_id varchar(32) NOT NULL default "" COMMENT '用户自定义订单ID，默认为空';"""

    table_name = 'slice_stop_{}'.format('example')
    if check_column(check_db_conn, TRADE_LOG_DB, table_name, 'client_id'):
        print("{} client id is exist.".format(table_name))
    else:
        sql_str = sql_str_format.format(table_name)
        cursor.execute(sql_str)
        conn.commit()

    table_name = 'slice_order_{}'.format('example')
    if check_column(check_db_conn, TRADE_LOG_DB, table_name, 'client_id'):
        print("{} client id is exist.".format(table_name))
    else:
        sql_str = sql_str_format.format(table_name)
        cursor.execute(sql_str)
        conn.commit()
    cursor.close()
    conn.close()
    check_db_conn.close()


def check_trade_history(order_type):
    db_conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db='information_schema')
    db_name_format = 'trade_history_{}'
    table_name_format = order_type + '_history_{}'
    for db_index in range(5):
        db_name = db_name_format.format(db_index)
        for table_index in range(100):
            order_table_name = table_name_format.format(table_index)
            if not check_column(db_conn, db_name, order_table_name, 'client_id'):
                print("db_name:{}, table_name:{}, column_name:{} not exist.".format(db_name, order_table_name, 'client_id'))

        order_table_name = table_name_format.format('example')
        if not check_column(db_conn, db_name, order_table_name, 'client_id'):
            print("db_name:{}, table_name:{}, column_name:{} not exist.".format(db_name, order_table_name, 'client_id'))
    db_conn.close()


def check_order_history():
    check_trade_history('order')


def check_stop_history():
    check_trade_history('stop')


def check_slice(order_type):
    db_conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=TRADE_LOG_DB)
    cursor = db_conn.cursor()
    query_sql_str = "select time from slice_history order by id desc limit 1"
    cursor.execute(query_sql_str)
    slice_history = cursor.fetchall()
    info_db_conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db='information_schema')
    order_table_name_format = 'slice_%s_{}' % order_type
    for history in slice_history:
        table_name = order_table_name_format.format(history[0])
        if not check_column(info_db_conn, TRADE_LOG_DB, table_name, 'client_id'):
             print("db_name:{}, table_name:{}, column_name:{} not exist.".format(TRADE_LOG_DB, table_name, 'client_id'))

    table_name = order_table_name_format.format('example')
    if not check_column(info_db_conn, TRADE_LOG_DB, table_name, 'client_id'):
        print("db_name:{}, table_name:{}, column_name:{} not exist.".format(TRADE_LOG_DB, table_name, 'client_id'))
    info_db_conn.close()
    db_conn.close()


def check_order_slice():
    check_slice('order')


def check_stop_slice():
    check_slice('stop')


def check_all():
    check_order_history()
    check_stop_history()
    check_order_slice()
    check_stop_slice()


def modify_all():
    modify_slice_order_table()
    modify_order_history_table()
    modify_stop_order_history_table()


def delete_order_history(order_type):
    for db_index in range(5):
        db_name = TRADE_HISTORY_FORMAT.format(db_index)
        conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=db_name)
        cursor = conn.cursor()
        table_sql_str_format = """alter table %s_history_{} drop client_id""" % order_type
        for table_id in range(100):
            sql_str = table_sql_str_format.format(table_id)
            cursor.execute(sql_str)
            conn.commit()
        sql_str = table_sql_str_format.format('example')
        cursor.execute(sql_str)
        conn.commit()
        cursor.close()
        conn.close()

def delete_order_history_table():
    delete_order_history('order')


def delete_stop_order_history_table():
    delete_order_history('stop')


def delete_slice_order_table():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=TRADE_LOG_DB)
    cursor = conn.cursor()
    order_table_sql_str_format = """alter table slice_order_{} drop client_id;"""
    stop_table_sql_str_format = """alter table slice_stop_{} drop client_id;"""
    query_sql_str = "select time from slice_history order by id desc limit 1"
    cursor.execute(query_sql_str)
    slice_history = cursor.fetchall()
    for history in slice_history:
        sql_str = order_table_sql_str_format.format(history[0])
        cursor.execute(sql_str)
        sql_str = stop_table_sql_str_format.format(history[0])
        cursor.execute(sql_str)
        conn.commit()

    sql_str = order_table_sql_str_format.format('example')
    cursor.execute(sql_str)
    sql_str = stop_table_sql_str_format.format('example')
    cursor.execute(sql_str)
    conn.commit()
    cursor.close()
    conn.close()


def delete_all():
    delete_order_history_table()
    delete_stop_order_history_table()
    delete_slice_order_table()


def print_usage():
    print("""
        python script.py modify  -- 添加字段client_id
                         check   -- 确认是否成功
                         delete  -- 删除字段client_id
    """)


def main(argv):
    if argv[0] == 'modify':
        modify_all()
    elif argv[0] == 'check':
        check_all()
    elif argv[0] == 'delete':
        delete_all()
    else:
        print_usage()


if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            print_usage()
            sys.exit(-1)
        main(sys.argv[1:])
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())

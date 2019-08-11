#encoding: utf-8
"""
    更新所有未执行的stop订单的option值
"""
import os
import sys
import pymysql
import json
import traceback


DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASSWD = 'shit'
TRADE_DB = 'trade_log'


def main():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=TRADE_DB)
    cursor = conn.cursor()
    sql_str = """SELECT `id`, `time` from `slice_history` ORDER BY `id` DESC LIMIT 1"""
    cursor.execute(sql_str)
    rows = cursor.fetchall()
    for row in rows:
        print("id: {}".format(row[0]))
        print("time: {}".format(row[1]))
        slice_time = row[1]
        slice_stop_table = "slice_stop_{}".format(slice_time)
        query_stop_sql_str = """SELECT `id`, `option` from `{}`""".format(slice_stop_table)
        cursor.execute(query_stop_sql_str)
        stop_rows = cursor.fetchall()
        for stop in stop_rows:
            order_option = stop[1] | 0x30 | 0x4
            update_sql_str = """
                update `{}` set `option` = {} where `id` = {}
            """.format(slice_stop_table, order_option, stop[0])
            cursor.execute(update_sql_str)
            conn.commit()

    cursor.close()
    conn.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
        print(traceback.format_exc())

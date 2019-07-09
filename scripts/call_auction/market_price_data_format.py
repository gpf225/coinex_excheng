#encoding: utf-8
"""
    make slice之后将slice_history最后的一条数据market_price个格式{'market_name': 'last'} 修改为:{'market_name': {'last': '1234', 'call_auction': false}}
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
    sql_str = """SELECT `id`, `time`, `end_oper_id`, `end_order_id`, `end_deals_id`, `market_price` from `slice_history` ORDER BY `id` DESC LIMIT 1"""
    cursor.execute(sql_str)
    rows = cursor.fetchall()
    for row in rows:
        print("id: {}".format(row[0]))
        print("time: {}".format(row[1]))
        print("end_oper_id: {}".format(row[2]))
        print("end_order_id: {}".format(row[3]))
        print("end_deals_id: {}".format(row[4]))
        print("market_price: {}".format(row[5]))
        query_market_price = json.loads(row[5])
        update_market_price = {}
        for market_name, last in query_market_price.items():
            update_market_price[market_name] = {
                'last': last,
                'call_auction': False
            }
        market_price = json.dumps(update_market_price)
        update_sql_str = """
            update slice_history set market_price = '{}' where id = {}
        """.format(pymysql.escape_string(market_price), row[0])
        print(update_sql_str, pymysql.escape_string(market_price), row[0])
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

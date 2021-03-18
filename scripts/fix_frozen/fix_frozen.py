
import pymysql
import requests
import decimal
import sys
import time

MYSQL_HOST = "8.129.115.68"
MYSQL_PORT = 44444
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_log"

'''
MYSQL_HOST = "coinexlog.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"
MYSQL_USER = "coinex"
MYSQL_PASS = "hp1sXMJftZWPO5bQ2snu"
MYSQL_DB = "trade_log"
MYSQL_PORT = 3306
'''

MYSQL_HOST_HIS = ["8.129.115.68", "8.129.115.68", "8.129.115.68", "8.129.115.68", "8.129.115.68"]
MYSQL_PORT_HIS = [44444, 44444, 44444, 44444, 44444]
MYSQL_USER_HIS = ["root", "root", "root", "root", "root"]
MYSQL_PASS_HIS = ["shit", "shit", "shit", "shit", "shit"]
MYSQL_DB_HIS = ["trade_history_0", "trade_history_1", "trade_history_2", "trade_history_3", "trade_history_4"]

'''
MYSQL_HOST_HIS = ["coinextradehistory0.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory1.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory2.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory3.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com", "coinextradehistory4.chprmbwjfj0p.ap-northeast-1.rds.amazonaws.com"]
MYSQL_PORT_HIS = [3306, 3306, 3306, 3306, 3306]
MYSQL_USER_HIS = ["root", "root", "root", "root", "root"]
MYSQL_PASS_HIS = ["6jh7QCaj4gX8QVx4T7j6", "7BG5CWFvPAOOdx99Gytn", "lsD9idvDE0b26W6V474M", "60yQcHSNB76PQtl7HvQA", "Hs7NMTIdG58Zk7sP68vD"]
MYSQL_DB_HIS =   ["trade_history_0", "trade_history_1", "trade_history_2", "trade_history_3", "trade_history_4"]
'''

MARKET_URL = "http://8.129.115.68:8000/internal/exchange/market/list"
TRADE_conn = ...
HIS_conn = dict()
HISTORY_DATABASE_NUM = 5
HISTORY_HASH_NUM = 100
ORDER_TABLE = ...
BALANCE_TABLE = ...
ZERO = decimal.Decimal("0")


def init_trade_conn():
    global TRADE_conn
    TRADE_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)


def init_history_conn(his_num=HISTORY_DATABASE_NUM):
    global HIS_conn
    for i in range(his_num):
        HIS_conn[i] = pymysql.connect(host=MYSQL_HOST_HIS[i], port=MYSQL_PORT_HIS[i], user=MYSQL_USER_HIS[i],
                                      passwd=MYSQL_PASS_HIS[i], db=MYSQL_DB_HIS[i])


def get_table_time():
    global TRADE_conn
    sql_str = f"select time from slice_history order by id desc limit 1"
    cursor = TRADE_conn.cursor()
    cursor.execute(sql_str)
    result = cursor.fetchone()[0]

    cursor.close()
    return result


def get_market_list_():
    market_list = dict()
    res = requests.get(MARKET_URL).json()
    for market_info in res['data']:
        market_list[market_info['name']] = {}
        market_list[market_info['name']]['stock'] = market_info['stock']['name']
        market_list[market_info['name']]['money'] = market_info['money']['name']
    return market_list


def get_frozen_balance_info():
    global TRADE_conn
    cursor = TRADE_conn.cursor()

    limit = 10000
    offset = 0
    balance_info = dict()
    while True:
        sql_str = f"select account, user_id, asset, balance from {BALANCE_TABLE} where " \
                  f"t = 2 and balance > 0 order by id limit {offset}, {limit}"
        cursor.execute(sql_str)
        records = cursor.fetchall()
        for account, user_id, asset, balance in records:
            key = f"{account}_{user_id}_{asset}"
            balance_info[key] = decimal.Decimal(balance)

        if len(records) < limit:
            break
        offset += limit

    cursor.close()
    return balance_info


def get_frozen_order_info():
    global TRADE_conn
    cursor = TRADE_conn.cursor()
    limit = 10000
    offset = 0
    market_list = get_market_list_()
    order_frozen_dict = dict()
    while True:
        sql_str = f"SELECT id, account, user_id, market, side, frozen FROM {ORDER_TABLE} WHERE frozen > 0 " \
                  f"ORDER BY user_id LIMIT {offset},{limit}"
        cursor.execute(sql_str)
        records = cursor.fetchall()

        for record_id, account, user_id, market, side, order_frozen in records:
            order_frozen = decimal.Decimal(order_frozen)
            if side == 1:
                asset = market_list[market]["stock"]  # stock
            else:
                asset = market_list[market]["money"]  # money
            key = f"{account}_{user_id}_{asset}"
            if key not in order_frozen_dict:
                order_frozen_dict[key] = [order_frozen, [record_id]]
            else:
                order_frozen_dict[key][0] += order_frozen
                order_frozen_dict[key][1].append(record_id)
        if len(records) < limit:
            break
        offset += limit

    cursor.close()
    return order_frozen_dict


def new_balance(account, user_id, asset, t):
    global TRADE_conn
    cursor = TRADE_conn.cursor()
    sql_str = f"insert into {BALANCE_TABLE} (account, user_id, asset, t, balance) values " \
              f"({account}, {user_id}, '{asset}', {t}, '0')"
    print(sql_str)
    if cursor.execute(sql_str) < 1:
        raise Exception("new_balance fail")
    cursor.close()


def get_balance(account, user_id, asset, t):
    global TRADE_conn
    cursor = TRADE_conn.cursor()
    sql_str = f"select balance from {BALANCE_TABLE} where " \
              f"account = {account} and user_id = {user_id} and asset = '{asset}' and t = {t}"
    print(sql_str)
    if cursor.execute(sql_str) < 1:
        new_balance(account, user_id, asset, t)
        balance = ZERO.copy_abs()
    else:
        balance = cursor.fetchone()[0]
    cursor.close()
    return decimal.Decimal(balance)


def set_balance(account, user_id, asset, t, balance):
    global TRADE_conn
    cursor = TRADE_conn.cursor()
    sql_str = f"update {BALANCE_TABLE} set balance = '{balance}' where " \
              f"account = {account} and user_id = {user_id} and asset = '{asset}' and t = {t}"
    if cursor.execute(sql_str) < 1:
        raise Exception("set balance error")
    cursor.close()


def del_balance(account, user_id, asset, t):
    global TRADE_conn
    cursor = TRADE_conn.cursor()
    sql_str = f"DELETE FROM {BALANCE_TABLE} WHERE account = {account} and user_id = {user_id} and asset = '{asset}' and t = {t}"
    print(sql_str)
    if cursor.execute(sql_str) != 1:
        raise Exception("set balance error")
    cursor.close()


def get_his_conn_table(user_id: int, db_num=5):
    db_index = (user_id % (db_num * HISTORY_HASH_NUM)) // HISTORY_HASH_NUM
    table_index = user_id % HISTORY_HASH_NUM
    table = f"order_history_{table_index}"
    db = HIS_conn[db_index]
    return db, table


def get_history_order_record(id_list: list):
    global TRADE_conn
    id_total = str(id_list)
    id_total = id_total.replace("[", "")
    id_total = id_total.replace("]", "")
    sql_str = "SELECT `id`, `create_time`, `update_time`, `user_id`, `account`, `option`, `market`, `source`, " \
              "`fee_asset`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, `deal_stock`, " \
              "`deal_money`, `money_fee`, `stock_fee`, `asset_fee`, `fee_discount`, `client_id` " \
              f"FROM {ORDER_TABLE} WHERE id in ({id_total}) and deal_stock > 0"
    print(sql_str)
    cursor = TRADE_conn.cursor()
    cursor.execute(sql_str)
    record_list = cursor.fetchall()

    cursor.close()
    return record_list


def get_sql_element(element):
    if isinstance(element, int):
        return str(element)
    elif isinstance(element, decimal.Decimal):
        return element.to_eng_string()
    elif isinstance(element, str):
        return f"'{element}'"
    else:
        return str(element)


def convert_record_2_str(record: tuple):
    result = str()
    first = True
    for element in record:
        if first is True:
            first = False
            result = f"{get_sql_element(element)}"
        else:
            result += f",{get_sql_element(element)}"
    return result


def append_order_history_batch(record_list: list, user_id):
    if len(record_list) == 0:
        return

    db_conn, table = get_his_conn_table(user_id)
    sql_str = f"INSERT INTO `{table}` " \
              "(`order_id`, `create_time`, `finish_time`, `user_id`, `account`, `option`, `market`, `source`, " \
              "`fee_asset`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, " \
              "`money_fee`, `stock_fee`, `asset_fee`, `fee_discount`, `client_id`) VALUES "
    print(sql_str)
    first = True
    for record in record_list:
        record_str = convert_record_2_str(record)
        if first is True:
            first = False
            sql_str += f"({record_str})"
        else:
            sql_str += f", ({record_str})"

    cursor = db_conn.cursor()
    if cursor.execute(sql_str) != len(record_list):
        raise Exception("append_order_history error")
    cursor.close()
    db_conn.commit()


def cancel_order_batch(id_list: list, user_id):
    # 订单取消前插入历史表
    record_list = get_history_order_record(id_list)
    append_order_history_batch(record_list, user_id)

    # 取消订单
    global TRADE_conn
    id_total = id_total = str(id_list)
    id_total = id_total.replace("[", "")
    id_total = id_total.replace("]", "")
    sql_str = f"delete from {ORDER_TABLE} where id in ({id_total})"
    print(sql_str)
    cursor = TRADE_conn.cursor()
    cursor.execute(sql_str)
    cursor.close()


def frozen_cancel(account, user_id, asset):
    global TRADE_conn
    available_balance = get_balance(account, user_id, asset, 1)
    frozen_balance = get_balance(account, user_id, asset, 2)
    new_available = available_balance + frozen_balance
    set_balance(account, user_id, asset, 1, new_available.to_eng_string())
    del_balance(account, user_id, asset, 2)
    TRADE_conn.commit()


def table_backup():
    global TRADE_conn
    cursor = TRADE_conn.cursor()
    sql_str = f"create table {ORDER_TABLE}_backup_{time.time()} as select * from {ORDER_TABLE}"
    cursor.execute(sql_str)
    sql_str = f"create table {BALANCE_TABLE}_backup_{time.time()} as select * from {BALANCE_TABLE}"
    cursor.execute(sql_str)
    cursor.close()


def main(operate):
    balance_frozen_dict = get_frozen_balance_info()
    order_frozen_dict = get_frozen_order_info()

    if operate == "check":
        diff = set(balance_frozen_dict.keys()).difference(set(order_frozen_dict.keys()))
        for key in diff:
            print("{} no order".format(key))

        for key, balance in order_frozen_dict.items():
            if key not in balance_frozen_dict:
                asset = key.split("_")[2]
                print("{} not frozen".format(key))
            elif balance_frozen_dict[key] != balance[0]:
                print("{} not equal, order frozen: {}, balance frozen: {}"
                      .format(key, balance[0], balance_frozen_dict[key]))

    if operate == "update":
        table_backup()
        init_history_conn(HISTORY_DATABASE_NUM)
        diff = set(balance_frozen_dict.keys()).difference(set(order_frozen_dict.keys()))
        for key in diff:
            print("{} no order".format(key))
            account, user_id, asset = key.split("_")
            account = int(account)
            user_id = int(user_id)
            frozen_cancel(account, user_id, asset)

        for key, balance in order_frozen_dict.items():
            if key not in balance_frozen_dict:
                print("{} not frozen".format(key))
                account, user_id, _ = key.split("_")
                account = int(account)
                user_id = int(user_id)
                cancel_order_batch(balance[1], user_id)
            elif balance_frozen_dict[key] != balance[0]:
                print("{} not equal, order frozen: {}, balance frozen: {}"
                      .format(key, balance[0], balance_frozen_dict[key]))
                account, user_id, asset = key.split("_")
                account = int(account)
                user_id = int(user_id)
                cancel_order_batch(balance[1], user_id)
                frozen_cancel(account, user_id, asset)


if __name__ == "__main__":
    init_trade_conn()
    timestamp = get_table_time()
    ORDER_TABLE = f"slice_order_{timestamp}"
    BALANCE_TABLE = f"slice_balance_{timestamp}"

    if len(sys.argv) != 2:
        print("check or update")
        exit(0)
    operate_ = sys.argv[1]
    if operate_ != "check" and operate_ != "update":
        print("only support check and update command line argv")
        exit(0)

    main(operate_)

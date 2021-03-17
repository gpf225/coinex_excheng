
import pymysql
import requests
import json
import decimal
import sys

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "shit"
MYSQL_DB = "trade_log"
db_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)
market_url = "http://127.0.0.1:8000/internal/exchange/market/list"

ORDER_TABLE = "slice_order_1615879602"
BALANCE_TABLE = "slice_balance_1615879602"
ZERO = decimal.Decimal("0")

exp_dict = dict()
asset_prec = dict()
asset_market_sides = dict()


def init_exp(count):
    global init_exp
    begin = "0."
    for i in range(count):
        exp = begin + "1"
        begin += "0"
        exp_dict[i+1] = decimal.Decimal(exp)


def get_table_time():
    sql_str = f"select time from slice_history order by id desc limit 1"
    cursor = db_conn.cursor()
    cursor.execute(sql_str)
    result = cursor.fetchone()[0]
    cursor.close()
    return result

# {"name": "CETBCH", "maker_fee_rate": "0.002", "taker_fee_rate": "0.002", "stock": {"name": "CET", "prec": 8}, "money": {"name": "BCH", "prec": 8}, "min_amount": "50", "account": 0},
def get_market_list(url):
    responese = requests.get(url)
    return responese.text


def get_market_list_info(market_list_str):
    # result1: {BTC: {BTCUSDT: 1}, {ETHBTC: 2}}
    # result2: {{BTCUSDT: (BTC,USDT)}, {ETHBTC: (ETH,BTC)}}
    result_balance = dict()
    result_order = dict()
    global asset_prec
    market_list = json.loads(market_list_str)
    market_list = market_list["data"]

    for market_info in market_list:
        market = market_info["name"]
        stock = market_info["stock"]["name"]
        money = market_info["money"]["name"]
        stock_prec = market_info["stock"]["prec"]
        money_prec = market_info["money"]["prec"]

        if stock not in asset_prec or asset_prec[stock] > stock_prec:
            asset_prec[stock] = stock_prec
        if money not in asset_prec or asset_prec[money] > money_prec:
            asset_prec[money] = money_prec

        result_order[market] = (stock, money)

        if stock in result_balance:
            result_balance[stock][market] = 1
        else:
            result_balance[stock] = dict()
            result_balance[stock][market] = 1 # ask
        if money in result_balance:
            result_balance[money][market] = 2
        else:
            result_balance[money] = dict()
            result_balance[money][market] = 2 # bid 
    return result_balance, result_order


def get_frozen_balance_info(table=BALANCE_TABLE):
    global db_conn
    sql_str = f"select account, user_id, asset, balance from {table} where t = 2 and balance > 0 order by user_id"
    cursor = db_conn.cursor()
    cursor.execute(sql_str)
    return cursor.fetchall()


def get_frozen_order_info(market_list: dict, table=ORDER_TABLE):
    global db_conn
    sql_str = f"SELECT account, user_id, market, side, SUM(frozen) FROM {table} where frozen > 0 group by account, user_id, market, side order by user_id"
    cursor = db_conn.cursor()
    cursor.execute(sql_str)
    records = cursor.fetchall()
    cursor.close()
    result = dict()
    for account, user_id, market, side, order_frozen in records:
        if side == 1:
            asset = market_list[market][0] # stock
        else:
            asset = market_list[market][1] # money
        key = f"{account}_{user_id}_{asset}"
        if key not in result:
            result[key] = []
        result[key].append((account, user_id, market, side, order_frozen))
    return result


def new_balance(account, user_id, asset, t, table=BALANCE_TABLE):
    global db_conn
    sql_str = f"insert into {table} (account, user_id, asset, t, balance) values ({account},{user_id},'{asset}',{t},0)"
    cursor = db_conn.cursor()
    if cursor.execute(sql_str) < 1:
        raise Exception("new_balance fail")
    cursor.close()


def get_balance(account, user_id, asset, t, table=BALANCE_TABLE):
    global db_conn
    global exp_dict
    global asset_prec
    sql_str = f"select balance from {BALANCE_TABLE} where account = {account} and user_id = {user_id} and asset = '{asset}' and t = {t}"
    cursor = db_conn.cursor()
    if cursor.execute(sql_str) < 1:
        new_balance(account, user_id, asset, t)
        balance = ZERO.copy_abs()
    else:
        balance = cursor.fetchone()[0]
    cursor.close()
    exp = exp_dict[asset_prec[asset]]
    return balance.quantize(exp, rounding = "ROUND_HALF_UP")


def set_balance(account, user_id, asset, t, balance, table=BALANCE_TABLE):
    global db_conn
    sql_str = f"update {table} set balance = '{balance}' where account = {account} and user_id = {user_id} and asset = '{asset}' and t = {t}"
    cursor = db_conn.cursor()
    if cursor.execute(sql_str) < 1:
        raise Exception("set balance error")
    cursor.close()


def cancel_order(account, user_id, market, side, table=ORDER_TABLE):
    global db_conn
    sql_str = f"delete from {table} where account = {account} and user_id = {user_id} and market = '{market}' and side = {side}"
    cursor = db_conn.cursor()
    cursor.execute(sql_str)
    cursor.close()

def cancel_all_order(market_sides: dict, account, user_id, table=ORDER_TABLE):
    global db_conn
    sql_str = f"delete from {table} where account = {account} and user_id = {user_id} and (" 
    first = True
    for market, side in market_sides.items():
        if first:
            first = False
            sql_str += f" (side = {side} and market = '{market}')"
        else:
            sql_str += f" or (side = {side} and market = '{market}')"
    sql_str += ")"
    cursor = db_conn.cursor()
    cursor.execute(sql_str)
    cursor.close()


def frozen_cancel(account, user_id, asset):
    global db_conn
    available_balance = get_balance(account, user_id, asset, 1)
    frozen_balance = get_balance(account, user_id, asset, 2)
    new_avaliable = available_balance + frozen_balance
    set_balance(account, user_id, asset, 1, new_avaliable.to_eng_string())
    set_balance(account, user_id, asset, 2, ZERO.to_eng_string())
    db_conn.commit()


# print to console
def check_balance_frozen(account, user_id, asset, balance_frozen, table=ORDER_TABLE):
    exp = exp_dict[asset_prec[asset]]
    order_frozen = order_frozen_total(asset_market_sides[asset], account, user_id)
    order_frozen = order_frozen.quantize(exp, rounding = "ROUND_HALF_UP")
    balance_frozen = balance_frozen.quantize(exp, rounding = "ROUND_HALF_UP")
    deviation = balance_frozen - order_frozen
    if deviation.copy_abs() > exp:
        print(f"account: {account}, user_id: {user_id}, asset: {asset}, balance_frozen: {balance_frozen} != order_frozen: {order_frozen}\n")


# update to database
def check_balance_frozen_real(account, user_id, asset, balance_frozen, table=ORDER_TABLE):
    exp = exp_dict[asset_prec[asset]]
    order_frozen = order_frozen_total(asset_market_sides[asset], account, user_id)
    order_frozen = order_frozen.quantize(exp, rounding = "ROUND_HALF_UP")
    balance_frozen = balance_frozen.quantize(exp, rounding = "ROUND_HALF_UP")
    deviation = balance_frozen - order_frozen
    if deviation.copy_abs() > exp:
        frozen_cancel(account, user_id, asset)


def order_frozen_total(market_sides: dict, account, user_id, table=ORDER_TABLE):
    global db_conn
    global asset_prec
    sql_str = f"select sum(frozen) from {table} where account = {account} and user_id = {user_id} and (" 
    first = True
    for market, side in market_sides.items():
        if first:
            first = False
            sql_str += f" (side = {side} and market = '{market}')"
        else:
            sql_str += f" or (side = {side} and market = '{market}')"
    sql_str += ")"
    cursor = db_conn.cursor()
    cursor.execute(sql_str)
    result = cursor.fetchone()[0]
    cursor.close()
    exp = exp_dict[asset_prec[asset]]
    if result is None:
        result = decimal.Decimal("0")
    result.quantize(exp, rounding = "ROUND_HALF_UP")
    return result


if __name__ == "__main__":
    init_exp(20)
    ORDER_TABLE = f"slice_order_{get_table_time()}"
    BALANCE_TABLE = f"slice_balance_{get_table_time()}"

    market_list = get_market_list(market_url)
    asset_market_sides, market_infos = get_market_list_info(market_list)

    operate = "update"
    if len(sys.argv) > 1:
        operate = sys.argv[1]
    
    if operate != "check" and operate != "update":
        print("only support check and update command line argv")
        exit(0)

    balance_infos = get_frozen_balance_info()
    order_infos = get_frozen_order_info(market_infos)

    if operate == "check":
        for account, user_id, asset, balance in balance_infos:
            key = f"{account}_{user_id}_{asset}"
            order_infos.pop(key, None)
            check_balance_frozen(account, user_id, asset, balance)
        for asset, records in order_infos.items():
            print(f"\nonly order frozen, asset: {asset}")
            for account, user_id, market, side, order_frozen in records:
                print(f"account: {account}, user_id: {user_id}, market: {market}, side: {side}, order_frozen: {order_frozen}")
            
    if operate == "update":
        for account, user_id, asset, balance in balance_infos:
            key = f"{account}_{user_id}_{asset}"
            order_infos.pop(key, None)
            check_balance_frozen_real(account, user_id, asset, balance)   
        for asset, records in order_infos.items():
            for account, user_id, market, side, _ in records:
                cancel_order(account, user_id, market, side)

import sys
import json
import time
import datetime
import requests
import pybybit
import schedule
import talib as ta
import numpy as np
from dateutil.parser import parse
from influxdb import InfluxDBClient, DataFrameClient

config = {}
with open(sys.argv[1]) as f:
    config = json.load(f)
apis = [config["api"], config["secret"]]

SYMBOL = config["symbol"]
TESTNET = config["testnet"]
leva = config["leva"]
logic_interval = config.get("logic_interval", 15)

bybit = pybybit.API(*apis, testnet=TESTNET)
BASE_SYMBOL = SYMBOL

# 直近の実現損益を取得する
def get_closed_pnl(hour=4):
    avg_exit_price = closed_pnl = side = None
    results = bybit.rest.inverse.private_trade_closedpnl_list(
        symbol=SYMBOL, start_time=int(time.time() - (60 * 60 * hour)), end_time=int(time.time()), limit=1
    )
    results = results.json()
    total_profit = 0
    try:
        for result in results["result"]["data"]:
            timestamp = datetime.datetime.fromtimestamp(result["created_at"]).isoformat()
            side = result["side"]
            closed_pnl = result["closed_pnl"]
            avg_exit_price = result["avg_exit_price"]
            # print(f'{timestamp}, {result["side"]} {result}')
    except:
        pass
    return side, closed_pnl, avg_exit_price

def adjust_position():
    while True:
        try:
            pos = bybit.rest.inverse.private_position_list(symbol=SYMBOL)
            pos = pos.json()["result"]
            break
        except Exception as e:
            message = "Get pos failed.:" + str(e)
            time.sleep(2)
            continue
    # testnet互換
    if type(pos) == list:
        pos = pos[0]["data"]
    entry_price = round(float(pos["entry_price"]))
    size = abs(int(pos["size"]))
    bybit.rest.inverse.private_order_cancelall(symbol=SYMBOL)
    order = bybit.rest.inverse.private_position_tradingstop(symbol=SYMBOL, stop_loss=entry_price)
    print(f"stop lossを修正します。ステータス: {order.json()['ret_msg']}")


# ポジション情報を取得する
def get_position():
    while True:
        try:
            pos = bybit.rest.inverse.private_position_list(symbol=SYMBOL)
            pos = pos.json()["result"]
            break
        except Exception as e:
            message = "Get pos failed.:" + str(e)
            time.sleep(2)
            continue
    while True:
        try:
            order = bybit.rest.inverse.private_order_list(symbol=SYMBOL, limit=1)
            order = order.json()["result"]["data"][0]
            break
        except Exception as e:
            message = "Get pos failed.:" + str(e)
            time.sleep(2)
            continue
    while True:
        try:
            tic = bybit.rest.inverse.public_tickers(symbol=SYMBOL)
            tic = tic.json()["result"][0]
            break
        except Exception as e:
            message = "Get tic failed.:" + str(e)
            time.sleep(2)
            continue

    # testnet互換
    if type(pos) == list:
        pos = pos[0]["data"]

    side = str(pos["side"])
    size = abs(int(pos["size"]))
    created_at = int(parse(order["created_at"]).timestamp()) if size > 0 else 0
    price = float(tic["mark_price"])
    realised_pnl = float(pos["realised_pnl"])
    unrealised_pnl = float(pos["unrealised_pnl"])
    wallet_balance = float(pos["wallet_balance"])
    cum_realised_pnl = float(pos["cum_realised_pnl"])
    profit = round(realised_pnl + unrealised_pnl, 4)
    leverage = float(pos["effective_leverage"])
    order_lot = int(price * (wallet_balance + unrealised_pnl) * leva)
    return [
        side,
        size,
        order_lot,
        wallet_balance,
        round(profit * price, 2),
        cum_realised_pnl,
        created_at,
        leverage,
        price,
    ]


# 分割指値注文
def split_order(symbol, side, order_price, qty, stop_loss, split_num=6):
    now = datetime.datetime.now()
    bybit.rest.inverse.private_order_cancelall(symbol=symbol)
    offset = (stop_loss - order_price) / split_num
    order_size = int(qty / split_num)
    price = order_price - 1
    split_order_price = []
    for i in range(split_num):
        price = round(price, 2)
        split_order_price.append(price)
        price += offset

    for price in split_order_price:
        bybit.rest.inverse.private_order_create(
            symbol=symbol,
            side=side,
            price=price,
            qty=order_size,
            stop_loss=stop_loss,
            order_type="Limit",
            time_in_force="GoodTillCancel",
        )
        print(f"[{now.isoformat()}][Limit Order] side:{side} price: {price} size: {order_size} stop_loss: {stop_loss}")


# 成行き注文
def market_order(symbol, side, order_price, qty, stop_loss, split_num=5):
    now = datetime.datetime.now()
    bybit.rest.inverse.private_order_cancelall(symbol=symbol)
    bybit.rest.inverse.private_order_create(
        symbol=symbol,
        side=side,
        # price=order_price,
        qty=qty,
        stop_loss=stop_loss,
        order_type="Market",
        time_in_force="GoodTillCancel",
    )
    print(f"[{now.isoformat()}][Market Order] side: {side} price: {order_price} size: {qty} stop_loss: {stop_loss}")

# influxdbからローソク足を自炊する
def _get_candle_dataframe(symbol, interval="60S", time_range="5d", database="bots"):
    client = DataFrameClient(host="influxdb", port=8086, database=database)
    sym = symbol.lower().replace('usd', '')
    query = f"SELECT {sym}_ltp as exec FROM market WHERE time > now() - {time_range} tz('Asia/Tokyo')"
    results = client.query(query)
    df = list(results.values())[0]
    df = df.tz_convert("Asia/Tokyo")
    df = df.resample(interval, offset="1H").ohlc().dropna()
    df.columns = ["open", "high", "low", "close"]
    df.dropna()
    return df

# influxdbからデータを集計する
def _get_dataset(query, key, database="bots"):
    client = DataFrameClient(host="influxdb", port=8086, database=database)
    results = client.query(query)
    df = list(results.values())[0]
    df = df.tz_convert("Asia/Tokyo")
    df = df.resample("60S").last()
    df.dropna()
    return float(df.tail(1)[key])

def get_ls_ratio_avg(symbol="BTCUSD", exchange="binanceF_global", num=100):
    key = f"ls_ratio_{exchange}"
    query = f"SELECT moving_average({key}, {num}) as {key}  FROM delta_ls_{symbol.lower()} WHERE time > now() - 60d tz('Asia/Tokyo')"
    ls_ratio = _get_dataset(query, key)
    return ls_ratio

def get_basis_avg(symbol="BTCUSD", num=100):
    key = symbol.replace("USD", "").lower()
    query = f"SELECT moving_average({key}_ltp, 100) - moving_average({key}_index_price, 100)  as {key}  FROM market WHERE time > now() - 4h tz('Asia/Tokyo')"
    basis = _get_dataset(query, key)
    return basis

def get_spot_future_value(symbol="BTCUSD", num=600):
    key = f"spot"
    query = f"SELECT (moving_average({key}_buy_value, {num}) / moving_average({key}_sell_value, {num})) as {key}  FROM trade_{symbol.lower().replace('usd', '')} WHERE time > now() - 60d tz('Asia/Tokyo')"
    spot = _get_dataset(query, key)
    key = f"future"
    query = f"SELECT (moving_average({key}_buy_value, {num}) / moving_average({key}_sell_value, {num})) as {key}  FROM trade_{symbol.lower().replace('usd', '')} WHERE time > now() - 60d tz('Asia/Tokyo')"
    future = _get_dataset(query, key)
    return spot, future

# 定期実行関数
def asset_info():
    now = datetime.datetime.now()
    (
        pos_side,
        pos_size,
        order_size,
        wallet_balance,
        profit,
        cum_realised_pnl,
        created_at,
        leverage,
        price,
    ) = get_position()
    ttl = int(time.time()) - created_at if created_at > 0 else -1
    # 現物 vs 先物
    spot, future = get_spot_future_value(symbol=BASE_SYMBOL)
    sd_trend = "LONG" if spot > future else "SHORT"
    # 先物 Long vs Short
    exchange = "binanceF_global"
    # exchange = "bybit"
    ls_ratio_avg_30 = get_ls_ratio_avg(BASE_SYMBOL, exchange=exchange, num=30)
    ls_ratio_avg_100 = get_ls_ratio_avg(BASE_SYMBOL, exchange=exchange, num=100)
    ls_trend = "LONG" if ls_ratio_avg_30 > ls_ratio_avg_100 else "SHORT"
    # インデックス価格との乖離
    basis = get_basis_avg(BASE_SYMBOL)
    # テクニカル指標
    df = _get_candle_dataframe(symbol=BASE_SYMBOL, interval="1H")
    rsi14 = ta.RSI(np.array(df["close"], dtype='f8'), timeperiod=14)[-1]
    print(
            f"[{str(now)}][{sys.argv[0]}][{sys.argv[1]}] symbol:{SYMBOL} side:{pos_side}, leverage: {leverage} profit: {profit} USD, total_profit: {cum_realised_pnl} ttl: {ttl} testnet: {TESTNET}"
    )
    print(f"[{str(now)}] BASE: {BASE_SYMBOL} 現先: {sd_trend} {exchange} LS: {ls_trend} インデックス価格との乖離: {round(basis, 4)} RSI(14): {rsi14}")

# ロジック本体
def logic(mode):
    try:
        now = datetime.datetime.now()
        # 現物 vs 先物
        spot, future = get_spot_future_value(symbol=BASE_SYMBOL)
        sd_trend = "LONG" if spot > future else "SHORT"
        # 先物 Long vs Short
        exchange = "binanceF_global"
        # exchange = "bybit"
        ls_ratio_avg_30 = get_ls_ratio_avg(BASE_SYMBOL, exchange=exchange, num=30)
        ls_ratio_avg_100 = get_ls_ratio_avg(BASE_SYMBOL, exchange=exchange, num=100)
        ls_trend = "LONG" if ls_ratio_avg_30 > ls_ratio_avg_100 else "SHORT"
        # インデックス価格との乖離
        basis = get_basis_avg(BASE_SYMBOL)
        # テクニカル指標
        df = _get_candle_dataframe(symbol=BASE_SYMBOL, interval="1h")
        rsi14 = ta.RSI(np.array(df["close"], dtype='f8'), timeperiod=14)[-1]
        # 口座情報を取得
        (
            pos_side,
            pos_size,
            order_size,
            wallet_balance,
            profit,
            cum_realised_pnl,
            created_at,
            leverage,
            price,
        ) = get_position()
        ttl = int(time.time()) - created_at if created_at > 0 else -1
        closed_side, closed_pnl, avg_exit_price = get_closed_pnl()
        # 4H ローソク足を取得
        # df = _get_candle_dataframe(symbol=SYMBOL, time_range="12h", interval="4h")
        # 1day ローソク足を取得
        df = _get_candle_dataframe(symbol=SYMBOL, time_range="2d", interval="1d")
        _open = np.array(df["open"], dtype='f8')[-1]
        _close = np.array(df["close"], dtype='f8')[-1]
        high = np.array(df["high"], dtype='f8')[-1]
        low = np.array(df["low"], dtype='f8')[-1]
        candle = _open < _close  # 陽線:True 陰線:False
        miss_price = high - _open if candle else _close - low
        realize = abs(_close - _open)
        miss_price = miss_price if miss_price > realize else realize # 実体が大きい場合は実体の長さを背にする。ヒゲが大きい場合はヒゲの長さを背にする
        # miss_price = miss_price * 2
        update_candle = (int(time.time()) % (60 * 60 * 4)) < 60  # ローソク更新から1分以内
        print(f"[{str(now)}] LOGIC: {sys.argv[0]} {sys.argv[1]}")
        print(f"[{str(now)}] BASE: {BASE_SYMBOL} 現先: {sd_trend} {exchange} LS: {ls_trend} インデックス価格との乖離: {round(basis, 4)} RSI(14): {rsi14}")
        print(f"===== pos_side: {pos_side}, candle: {candle}, update_candle: {update_candle} =====")
        print(f"===== 4H ローソク open: {_open}, high: {high}, low: {low} close: {_close} =====")

        # 利確
        if sd_trend == "LONG" and pos_side == "Sell":
            market_order(symbol=SYMBOL, side="Buy", order_price=price, qty=pos_size, stop_loss=None)
            pos_side = "None"
        if sd_trend == "SHORT" and pos_side == "Buy":
            market_order(symbol=SYMBOL, side="Sell", order_price=price, qty=pos_size, stop_loss=None)
            pos_side = "None"

        # 半分利確
        if pos_side == "Sell" and rsi14 <= 40 and leverage > 0.5:
            market_order(symbol=SYMBOL, side="Buy", order_price=price, qty=int(pos_size/2), stop_loss=None)
        if pos_side == "Buy" and rsi14 >= 60 and leverage > 0.5:
            market_order(symbol=SYMBOL, side="Sell", order_price=price, qty=int(pos_size/2), stop_loss=None)

        # 新規エントリー
        if sd_trend == ls_trend == "LONG" and rsi14 <= 45:
            stop_loss = round(low - miss_price, 2)
            if pos_side == "None" and avg_exit_price is not None and price > avg_exit_price and closed_side == "Sell":
                # 狼狽を防止
                print("====== 前回損切り値よりも高いです ======")
                return
            if pos_side == "None" and not update_candle and mode != "SHORT":
                # 新規注文は分割指値注文
                split_order(symbol=SYMBOL, side="Buy", order_price=price, qty=order_size, stop_loss=stop_loss)
                return

        if sd_trend == ls_trend == "SHORT" and rsi14 >= 55:
            stop_loss = round(high + miss_price, 2)
            if pos_side == "None" and avg_exit_price is not None and price < avg_exit_price and closed_side == "Buy":
                # 狼狽を防止
                print("====== 前回損切り値よりも安いです ======")
                return
            if pos_side == "None" and not update_candle and mode != "LONG":
                # 新規注文は分割指値注文
                split_order(symbol=SYMBOL, side="Sell", order_price=price, qty=order_size, stop_loss=stop_loss)
                return

        # SLの調整
        if ttl > 28800 and pos_side == "Buy":
            adjust_position()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    print(f"START {sys.argv[0]} {sys.argv[1]}")
    mode = "BOTH" # LONG or SHORT or BOTH
    logic(mode)
    try:
        asset_info()
    except Exception as e:
        print(e)
    t = logic_interval * 60
    wait_time = int(t - time.time() % t) + 10
    print(f"sleep {wait_time}[sec]")
    time.sleep(wait_time)
    logic(mode)

    # 定期的にポジションとマーケット情報を取得して表示する
    schedule.every(1).minutes.do(asset_info)
    schedule.every(logic_interval).minutes.at(":10").do(logic, mode)

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(e)
        time.sleep(0.01)


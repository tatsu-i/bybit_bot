# coding: utf-8
#!/usr/bin/python3
import os
import sys
import time
import requests
import schedule
import ccxt
import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, DataFrameClient


def _check_dataset(query, key, database="bots"):
    client = DataFrameClient(host="influxdb", port=8086, database=database)
    results = client.query(query)
    df = list(results.values())[0]
    df = df.tz_convert("Asia/Tokyo")
    df = df.resample("60S").last()
    df.dropna()
    offset_time = time.time() - parse(str(df.tail(1).index[0])).timestamp()
    return offset_time


def check_spot_future_value():
    for symbol in ["BTCUSD", "ETHUSD"]:
        offset_time = 0
        key = f"spot"
        query = f"SELECT {key}_buy_value as {key} FROM trade_{symbol.lower().replace('usd', '')} WHERE time > now() - 60d tz('Asia/Tokyo')"
        offset_time += _check_dataset(query, key)
        key = f"future"
        query = f"SELECT {key}_buy_value as {key} FROM trade_{symbol.lower().replace('usd', '')} WHERE time > now() - 60d tz('Asia/Tokyo')"
        offset_time += _check_dataset(query, key)
        if offset_time > 600:
            print(f"waring offset_time: {offset_time}")
            sys.exit(1)


def get_hashrate():
    hashrate = 0
    try:
        response = requests.get("https://blockchain.info/q/hashrate")
        hashrate = int(response.text)
    except Exception as e:
        print(e)
    return hashrate


def get_bybit_ltp(symbol="BTC/USD"):
    ltp = ltp_futures_u = ltp_futures_z = 0
    bybit = ccxt.bybit()
    ltp = bybit.fetch_ticker(symbol=symbol)["last"]
    mark_price = float(bybit.fetch_ticker(symbol=symbol)["info"]["mark_price"])
    index_price = float(bybit.fetch_ticker(symbol=symbol)["info"]["index_price"])
    ltp_futures_u = 0#float(bybit.fetch_ticker(symbol=f"{symbol.replace('/USD', 'USD')}U21")["last"])
    ltp_futures_z = 0#float(bybit.fetch_ticker(symbol=f"{symbol.replace('/USD', 'USD')}Z21")["last"])
    return ltp, ltp_futures_u, ltp_futures_z, mark_price, index_price


def get_bybit_info(symbol="BTC/USD"):
    bybit = ccxt.bybit()
    info = bybit.fetch_ticker(symbol=symbol)["info"]
    return info


def _get_oi_binanceF(symbol):
    url = f"https://www.binance.com/futures/data/openInterestHist?symbol={symbol}T&period=5m&limit=10"
    sumOpenInterest = 0
    sumOpenInterestValue = 0
    while True:
        try:
            r = requests.get(url)
            j = r.json()
            sumOpenInterest = float(j[-1]["sumOpenInterest"])
            sumOpenInterestValue = float(j[-1]["sumOpenInterestValue"])
            break
        except Exception as e:
            print(e)
            time.sleep(5)
    return sumOpenInterest, sumOpenInterestValue


def _get_delta_ls_binanceF(symbol, user="global", interval="4h"):
    delta_ls = 0
    url = f"https://www.binance.com/futures/data/{user}LongShortAccountRatio?symbol={symbol}T&period=5m&limit=500"
    offset = -49
    while True:
        try:
            r = requests.get(url)
            j = r.json()
            if interval == "1h":
                offset = -13
            if interval == "4h":
                offset = -49
            if interval == "8h":
                offset = -98
            delta_ls = float(j[-1]["longAccount"]) - float(j[offset]["longAccount"])
            break
        except Exception as e:
            print(e)
            time.sleep(5)
    return delta_ls


def _get_longShortRatio_binanceF(symbol, user="global"):
    longShortRatio = 0.0
    url = f"https://www.binance.com/futures/data/{user}LongShortAccountRatio?symbol={symbol}T&period=5m&limit=500"
    while True:
        try:
            r = requests.get(url)
            j = r.json()
            longShortRatio = float(j[-1]["longShortRatio"])
            break
        except Exception as e:
            print(e)
            time.sleep(5)
    return longShortRatio


def _get_longShortRatio_bybit(symbol):
    longShortRatio = 0.0
    url = f"https://api.bybit.com/v2/public/account-ratio?symbol={symbol}&period=5min"
    while True:
        try:
            r = requests.get(url)
            j = r.json()["result"]
            longShortRatio = float(j[-1]["buy_ratio"]) / float(j[-1]["sell_ratio"])
            break
        except Exception as e:
            print(e)
            time.sleep(5)
    return longShortRatio


def _get_delta_ls_bybit(symbol, interval="4h"):
    delta_ls = 0
    url = f"https://api.bybit.com/v2/public/account-ratio?symbol={symbol}&period=5min"
    offset = -49
    while True:
        try:
            r = requests.get(url)
            j = r.json()["result"]
            if interval == "1h":
                offset = -13
            if interval == "4h":
                offset = -49
            if interval == "8h":
                offset = -98
            delta_ls = float(j[-1]["buy_ratio"]) - float(j[offset]["buy_ratio"])
            break
        except Exception as e:
            print(e)
            time.sleep(5)
    return delta_ls


def avg(value):
    return sum(value) / len(value)


def get_marginmarketcap(symbol="BTC", futures=False):
    dtype = 0 if futures else 1
    r = requests.get(f"https://www.bybt.com/api/api/futures/v2/marginMarketCap?symbol={symbol}&type={dtype}")
    data = r.json()
    if data["msg"] != "success":
        return
    longRate = []
    shortRate = []
    openInterest = []
    longVolUsd = []
    shortVolUsd = []
    for d in data["data"][symbol]:
        longRate.append(d["longRate"])
        shortRate.append(d["shortRate"])
        longVolUsd.append(d["longVolUsd"])
        shortVolUsd.append(d["shortVolUsd"])
        openInterest.append(d["openInterest"])

    result = {}
    result["longRate"] = avg(longRate)
    result["shortRate"] = avg(shortRate)
    result["longVolUsd"] = avg(longVolUsd)
    result["shortVolUsd"] = avg(shortVolUsd)
    result["openInterest"] = avg(openInterest)
    return result


# ---------------
# Job登録関数
# ---------------
def post_indicator():
    client = InfluxDBClient(host="influxdb", port=8086, database="bots")
    bybit_info_btc = get_bybit_info(symbol="BTC/USD")
    bybit_info_eth = get_bybit_info(symbol="ETH/USD")
    btc_oi, btc_oi_value = _get_oi_binanceF(symbol="BTCUSD")
    eth_oi, eth_oi_value = _get_oi_binanceF(symbol="ETHUSD")
    data = [
        {
            "measurement": "indicator",
            "fields": {
                "btc_predicted_fr": float(bybit_info_btc["predicted_funding_rate"]) * 100,
                "eth_predicted_fr": float(bybit_info_eth["predicted_funding_rate"]) * 100,
                "btc_oi": btc_oi,
                "eth_oi": eth_oi,
                "btc_oi_value": btc_oi_value,
                "eth_oi_value": eth_oi_value,
            },
        }
    ]
    client.write_points(data)


def post_ticker():
    client = InfluxDBClient(host="influxdb", port=8086, database="bots")
    btc_ltp, btc_ltp_futures_u, btc_ltp_futures_z, btc_mark_price, btc_index_price = get_bybit_ltp(symbol="BTC/USD")
    eth_ltp, eth_ltp_futures_u, eth_ltp_futures_z, eth_mark_price, eth_index_price = get_bybit_ltp(symbol="ETH/USD")
    data = [
        {
            "measurement": "market",
            "fields": {
                "btc_ltp": btc_ltp,
                "eth_ltp": eth_ltp,
                "btc_mark_price": btc_mark_price,
                "eth_mark_price": eth_mark_price,
                "btc_index_price": btc_index_price,
                "eth_index_price": eth_index_price,
                # "btc_ltp_futures_u": btc_ltp_futures_u,
                # "btc_ltp_futures_z": btc_ltp_futures_z,
                # "eth_ltp_futures_u": eth_ltp_futures_u,
                # "eth_ltp_futures_z": eth_ltp_futures_z,
            },
        }
    ]
    client.write_points(data)


def post_delta_ls():
    for symbol in ["BTCUSD", "ETHUSD"]:
        client = InfluxDBClient(host="influxdb", port=8086, database="bots")
        delta_ls_binanceF_global_8h = _get_delta_ls_binanceF(symbol, user="global", interval="8h")
        delta_ls_binanceF_top_8h = _get_delta_ls_binanceF(symbol, user="top", interval="8h")
        delta_ls_binanceF_global_4h = _get_delta_ls_binanceF(symbol, user="global", interval="4h")
        delta_ls_binanceF_top_4h = _get_delta_ls_binanceF(symbol, user="top", interval="4h")
        delta_ls_binanceF_global_1h = _get_delta_ls_binanceF(symbol, user="global", interval="1h")
        delta_ls_binanceF_top_1h = _get_delta_ls_binanceF(symbol, user="top", interval="1h")
        ls_ratio_binanceF_top = _get_longShortRatio_binanceF(symbol, user="top")
        ls_ratio_binanceF_global = _get_longShortRatio_binanceF(symbol, user="global")
        ls_ratio_bybit = _get_longShortRatio_bybit(symbol)

        delta_ls_bybit_4h = _get_delta_ls_bybit(symbol, interval="4h")
        # _get_delta_ls_bybit_8h = _get_delta_ls_bybit(symbol, interval="8h")
        data = [
            {
                "measurement": f"delta_ls_{symbol.lower()}",
                "fields": {
                    "delta_ls_binanceF_global_8h": delta_ls_binanceF_global_8h,
                    "delta_ls_binanceF_top_8h": delta_ls_binanceF_top_8h,
                    "delta_ls_binanceF_global_4h": delta_ls_binanceF_global_4h,
                    "delta_ls_binanceF_top_4h": delta_ls_binanceF_top_4h,
                    "delta_ls_binanceF_global_1h": delta_ls_binanceF_global_1h,
                    "delta_ls_binanceF_top_1h": delta_ls_binanceF_top_1h,
                    "delta_ls_bybit_4h": delta_ls_bybit_4h,
                    "ls_ratio_binanceF_top": ls_ratio_binanceF_top,
                    "ls_ratio_binanceF_global": ls_ratio_binanceF_global,
                    "ls_ratio_bybit": ls_ratio_bybit,
                },
            }
        ]
        client.write_points(data)


def post_marketcap():
    client = InfluxDBClient(host="influxdb", port=8086, database="bots")
    data = [{"measurement": "marginmarketcap", "fields": get_marginmarketcap(symbol="BTC")}]
    client.write_points(data)
    data = [{"measurement": "marginmarketcap_futures", "fields": get_marginmarketcap(symbol="BTC", futures=True)}]
    client.write_points(data)


if __name__ == "__main__":
    post_indicator()
    post_ticker()
    post_delta_ls()
    time.sleep(300 - time.time() % 300)

    # schedule.every(1).minutes.do(post_marketcap)
    schedule.every(1).minutes.do(post_indicator)
    schedule.every(5).minutes.at(":00").do(post_delta_ls)
    schedule.every(5).seconds.do(post_ticker)
    schedule.every(1).minutes.do(check_spot_future_value)

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(e)
        time.sleep(1)

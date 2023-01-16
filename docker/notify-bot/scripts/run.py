import os
import re
import sys
import json
import time
import ccxt
import random
import tweepy
import requests
import schedule
import pybybit
from datetime import datetime, timedelta


asset_config = {}

# bybit api設定
with open(sys.argv[1]) as f:
    asset_config = json.load(f)
TESTNET = asset_config["testnet"]


def line_notify(message):
    try:
        url = "http://127.0.0.1:8080/post/line"
        headers = {"Content-Type": "application/json"}
        payload = {"message": message}
        requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
        )
    except Exception as e:
        print(e)
    print(f"[{datetime.now().isoformat()}] {message}")


def tweet_text(text):
    try:
        url = "http://127.0.0.1:8080/post/tweet"
        headers = {"Content-Type": "application/json"}
        payload = {"message": message}
        requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
        )
    except Exception as e:
        print(e)
    print(f"[{datetime.now().isoformat()}] {message}")


def get_profit(bybit_apis):
    bitflyer = ccxt.bitflyer()
    daily_profit = 0
    total_profit = 0
    for bybit_config in bybit_apis:
        bybit = ccxt.bybit()
        bybit.apiKey = bybit_config["api"]
        bybit.secret = bybit_config["secret"]
        # bybit.set_sandbox_mode(True)
        res = bybit.fetch_balance()
        for symbol in ['BTC', 'ETH']:
            info = res["info"]["result"]
            realised_pnl = float(info[symbol]["realised_pnl"])
            # unrealised_pnl = float(info[symbol]["unrealised_pnl"])
            unrealised_pnl = 0
            cum_realised_pnl = float(info[symbol]["cum_realised_pnl"])
            price = bitflyer.fetch_ticker(symbol=f"{symbol}/JPY")['last']
            daily_profit += int((realised_pnl + unrealised_pnl) * price)
            total_profit += int((realised_pnl + unrealised_pnl + cum_realised_pnl) * price)
    return total_profit, daily_profit

def get_daily_profit():
    cum_pnl, pnl = get_profit(asset_config["bybit"])
    message = f"本日の利益は{str(pnl).replace('-','マイナス')}円です。"
    line_notify(message)


def callback(message, ws):
    try:
        message = json.loads(message)
        topic = message.get("topic")
        if topic == "order":
            for data in message["data"]:
                side = "ロング" if data["side"] == "Buy" else "ショート"
                symbol = data["symbol"].replace("USD", "/USD")
                price = data["last_exec_price"]
                print(data)
                if data["close_on_trigger"] == False:
                    message = f"{symbol}を{side}でエントリーしました。"
                    if data["order_type"] == "Limit":
                        message = f"{symbol}を{side}で指値注文しました。"
                    if data["order_status"] == "Filled":
                        message = f"{symbol} 注文が約定しました。"
                    if data["order_status"] == "Cancelled":
                        message = f"{symbol} 注文をキャンセルしました。"
                    line_notify(message)
                else:
                    message = f"{symbol}のポジションを損切りしました。"
                    line_notify(message)
                break
    except Exception as e:
        print(e)


for bybit_config in asset_config["bybit"]:
    apis = [bybit_config["api"], bybit_config["secret"]]
    bybit = pybybit.API(*apis, testnet=TESTNET)
    bybit.ws.add_callback(callback)
    bybit.ws.run_forever_inverse(topics=[f"order"])
    get_daily_profit()
    schedule.every(1).hours.at(":00").do(get_daily_profit)

while True:
    try:
        schedule.run_pending()
    except Exception as e:
        print(e)
    time.sleep(1)

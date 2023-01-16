import os
import re
import sys
import json
import time
import requests
import schedule
import pybybit
from datetime import datetime, timedelta


asset_config = {}
# bybit api設定
with open(sys.argv[1]) as f:
    bybit_config = json.load(f)
TESTNET = bybit_config["testnet"]

# lineに通知する
def line_notify(message):
    try:
        env = "開発" if TESTNET else "本番"
        if not TESTNET:
            url = "http://notify-bot:8080/post/line"
            headers = {"Content-Type": "application/json"}
            payload = {"message": message}
            requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
            )
    except Exception as e:
        print(e)
    print(f"[{datetime.now().isoformat()}] {env} {message}")


def callback(message, ws):
    try:
        message = json.loads(message)
        topic = message.get("topic")
        if topic == "order":
            for data in message["data"]:
                side = "ロング" if data["side"] == "Buy" else "ショート"
                close_side = "Buy" if data["side"] == "Sell" else "Sell"
                order_symbol = data["symbol"]
                symbol = order_symbol.replace("USD", "/USD")
                price = data["last_exec_price"]
                order_id = data["order_id"]
                order_type = data["order_type"]
                order_status = data["order_status"]
                create_type = data["create_type"]
                qty = data["qty"]
                stop_loss = data["stop_loss"]
                reduce_only = data["reduce_only"]
                order_type_str = "成行" if order_type == "Market" else "指値"
                print(data)
                message = f"{symbol} {create_type} {side}の{order_type}オーダーが{order_status}の状態で実行されました。"
                if create_type == "CreateByUser":
                    if order_type == "Limit" and order_status == "New":
                        message = f"{symbol}を{side}で指値注文しました。"
                    if order_type == "Limit" and order_status == "PartiallyFilled":
                        message = f"{symbol} {side}の指値注文が部分約定しました。"
                    if order_status == "Filled":
                        pos = bybit.rest.inverse.private_position_list(symbol=order_symbol)
                        pos = pos.json()["result"]
                        leverage = float(pos["effective_leverage"])
                        entry_price = float(pos["entry_price"])
                        message = f"{symbol} {side} {order_type_str}注文が全て約定しました。レバレッジ: {leverage}倍"
                        if leverage >= 1.2 and order_type == "Limit" and reduce_only == False:
                            try:
                                order = bybit.rest.inverse.private_order_create(
                                    symbol=order_symbol,
                                    side=close_side,
                                    price=round(entry_price, 2),
                                    order_type="Limit",
                                    qty=qty,
                                    time_in_force="GoodTillCancel",
                                    reduce_only=True,
                                    close_on_trigger=True,
                                    order_link_id=order_id
                                )
                                print(f"order result: {order.json()}")
                            except Exception as e:
                                print(e)
                if create_type == "CreateByClosing" and order_status == "New":
                    message = f"{symbol}を{side}で建値調整の指値注文をしました。"
                if create_type == "CreateByClosing" and order_status == "Filled":
                    message = f"{symbol}を{side}で建値調整の指値注文が約定しました。"
                if create_type == "CreateByClosing" and order_status == "PartiallyFilled":
                    message = f"{symbol}を{side}で建値調整の指値注文が部分約定しました。"
                if create_type == "CreateByTrailingProfit":
                    message = f"{symbol} トレーリングプロフィット注文が実行されました。"
                if create_type == "CreateByTrailingStop":
                    message = f"{symbol} トレーリングストップ注文が実行されました。"
                if create_type == "CreateByPartialTakeProfit":
                    message = f"{symbol}のポジションを部分利確しました。"
                if create_type == "CreateByTakeProfit":
                    message = f"{symbol}のポジションを利確しました。"
                if create_type == "CreateByPartialStopLoss":
                    message = f"{symbol}のポジションを部分的に損切りしました。"
                if create_type == "CreateByStopLoss":
                    message = f"{symbol}のポジションを損切りしました。"
                if order_status == "Cancelled":
                    message = f"{symbol} 注文をキャンセルしました。"
                line_notify(message)
                break
    except Exception as e:
        line_notify(f"bybit apiの仕様が変更された可能性があります。callback error")
        print(e)


apis = [bybit_config["api"], bybit_config["secret"]]
bybit = pybybit.API(*apis, testnet=TESTNET)
bybit.ws.add_callback(callback)
topics = ["order"]
wsurl = bybit.ws._MAINNET_INVERSE if not bybit.ws._testnet else bybit.ws._TESTNET_INVERSE
while True:
    try:
        line_notify("bybit websocketに接続しました。")
        bybit.ws._loop(wsurl, topics)
    except Exception as e:
        print(e)
        line_notify("bybit websocketが停止しました。")
    time.sleep(10)

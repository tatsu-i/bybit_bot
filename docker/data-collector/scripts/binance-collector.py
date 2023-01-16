# coding: utf-8
#!/usr/bin/python3
import os
import sys
import json
import time
import websocket
import threading
from datetime import datetime, timezone, timedelta
from influxdb import InfluxDBClient

client = InfluxDBClient(host="influxdb", port=8086, database="bots")


class Trade:
    def __init__(self, url):
        self.url = url
        self.thread = threading.Thread(target=lambda: self.run())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        while True:
            try:
                # websocket.enableTrace(True)
                self.ws = websocket.WebSocketApp(
                    self.url,
                    on_open=lambda ws: self.on_open(ws),
                    on_close=lambda ws: self.on_close(ws),
                    on_message=lambda ws, msg: self.on_message(ws, msg),
                    on_error=lambda ws, err: self.on_error(ws, err),
                )
                self.ws.run_forever()
            except Exception as e:
                print(e)

    def on_open(self, ws):
        pass

    def on_close(self, ws):
        pass

    def on_message(self, ws, msg):
        pass

    def on_error(self, ws, err):
        sys.exit(1)
        pass


class BinanceTrade(Trade):
    def __init__(self, symbol, endpoint, trade_type):
        self.symbol = symbol.lower().replace("/", "")
        self.trade_type = trade_type
        super().__init__(f"{endpoint}/{self.symbol}@trade")
        self.buy_value = 0.0
        self.sell_value = 0.0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.last_hour = -1
        self.last_minute = -1

    def on_message(self, ws, msg):
        msg = json.loads(msg)
        if msg["e"] == "trade":
            ts = msg["T"] / 1000
            now = datetime.fromtimestamp(ts)
            # 1分間隔で送信する
            if self.last_minute != now.minute:
                if not (self.sell_value == self.buy_value == 0):
                    data = [
                        {
                            "measurement": f"trade_{self.symbol.replace('usdt', '')}",
                            "fields": {
                                f"{self.trade_type}_buy_value": self.buy_value,
                                f"{self.trade_type}_sell_value": self.sell_value,
                                f"{self.trade_type}_buy_vs_sell_value": float(self.buy_value / self.sell_value),
                                f"{self.trade_type}_buy_volume": self.buy_volume,
                                f"{self.trade_type}_sell_volume": self.sell_volume,
                            },
                        }
                    ]
                    client.write_points(data)
                self.buy_volume = 0
                self.sell_volume = 0
                self.last_minute = now.minute

            # 時間が更新されたら初期化する
            if self.last_hour != now.hour:
                print(
                    f"[{now.isoformat()}] {self.last_hour}H {self.trade_type} {self.symbol} buy_value: {self.buy_value} sell_value: {self.sell_value}"
                )
                self.buy_value = 0
                self.sell_value = 0
                self.last_hour = now.hour

            price = float(msg["p"])
            size = float(msg["q"])
            value = float(size) * float(price)
            side = "buy"
            if msg["m"]:
                side = "sell"
            if side == "buy":
                self.buy_value += value
                self.buy_volume += size
            if side == "sell":
                self.sell_value += value
                self.sell_volume += size


if __name__ == "__main__":
    SPOT_ENDPOINT = "wss://stream.binance.com:9443/ws"  # Binance 現物
    FUTURE_ENDPOINT = "wss://fstream.binance.com/ws"  # Binance future
    BinanceTrade("BTC/USDT", SPOT_ENDPOINT, "spot")
    BinanceTrade("ETH/USDT", SPOT_ENDPOINT, "spot")
    BinanceTrade("BTC/USDT", FUTURE_ENDPOINT, "future")
    BinanceTrade("ETH/USDT", FUTURE_ENDPOINT, "future")
    while True:
        time.sleep(1)

import os
import re
import sys
import json
import tweepy
import requests
from datetime import datetime, timedelta

from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


# 設定読み込み
with open("/conf/notify.json") as f:
    config = json.load(f)

tweet_config = config["twitter"]
API_KEY = tweet_config.get("api_key", "")
API_SECRET_KEY = tweet_config.get("api_secret_key", "")
ACCESS_TOKEN = tweet_config.get("access_token", "")
ACCESS_TOKEN_SECRET = tweet_config.get("access_token_secret", "")

# LINE アクセストークン
LINE_ACCESS_TOKEN = config.get("line_access_token")


class Item(BaseModel):
    message: str


@app.post("/post/line")
def post_line(item: Item):
    try:
        url = "https://notify-api.line.me/api/notify"
        headers = {"Authorization": "Bearer " + LINE_ACCESS_TOKEN}
        payload = {"message": item.message}
        requests.post(
            url,
            headers=headers,
            params=payload,
        )
    except Exception as e:
        print(e)
    return {"status": "ok"}


@app.post("/post/tweet")
def post_tweet(item: Item):
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    api.update_status(status=item.message)
    return {"status": "ok"}

#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
print requests.get("http://127.0.0.1:9000/v1/market/list").text
print requests.get("http://127.0.0.1:9000/v1/market/ticker?market=cetbch").text
print requests.get("http://127.0.0.1:9000/v1/market/ticker/all").text
print requests.get("http://127.0.0.1:9000/v1/market/depth?market=cetbch&merge=0").text
print requests.get("http://127.0.0.1:9000/v1/market/deals?market=cetbch").text
print requests.get("http://127.0.0.1:9000/v1/market/kline?market=cetbch&type=1hour").text


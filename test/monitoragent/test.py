#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import time
import requests

def call(method, *params):
    data = {'method': method, 'params': params, 'id': int(time.time() * 1000)}
    r = requests.post('http://127.0.0.1:6666/', data=json.dumps(data))
    print r.text

call("monitor.inc", "server", "test_inc", "127.0.0.1", 1)
call("monitor.inc", "server", "test_inc", "127.0.0.1", 4)
call("monitor.set", "server", "test_set", "127.0.0.1", 5)

#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from redis.sentinel import Sentinel

SENTINEL_LIST = [("127.0.0.1", 26381), ("127.0.0.1", 26382), ("127.0.0.1", 26383)]
SENTINEL_NAME = "mymaster"

def main():
    sentinel = Sentinel(SENTINEL_LIST)
    client = sentinel.master_for(SENTINEL_NAME)
    for key in client.scan_iter("m:*", 100):
        print key
        client.delete(key)

if __name__ == "__main__":
    main()

#!/bin/bash

killall -s SIGQUIT matchengine.exe
sleep 1
./matchengine.exe config.json

sleep 2
echo "update_market_list" | nc 127.0.0.1 7417

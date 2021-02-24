#!/bin/bash

killall -s SIGQUIT historyreader.exe
sleep 1
./historyreader.exe config.json

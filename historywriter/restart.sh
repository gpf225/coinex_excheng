#!/bin/bash

killall -s SIGQUIT historywriter.exe
sleep 1
./historywriter.exe config.json

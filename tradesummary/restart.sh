#!/bin/bash

killall -s SIGQUIT tradesummary.exe
sleep 1
./tradesummary.exe config.json

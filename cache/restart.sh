#!/bin/bash

killall -s SIGQUIT cachecenter.exe
sleep 1
./cache.exe config.json

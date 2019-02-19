#!/bin/bash

killall -s SIGQUIT cache.exe
sleep 1
./cache.exe config.json

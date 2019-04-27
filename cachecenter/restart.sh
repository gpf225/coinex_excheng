#!/bin/bash

killall -s SIGQUIT cachecenter.exe
sleep 1
./cachecenter.exe config.json

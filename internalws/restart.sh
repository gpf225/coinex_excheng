#!/bin/bash

killall -s SIGQUIT internalws.exe
sleep 1
./internalws.exe config.json

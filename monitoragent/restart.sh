#!/bin/bash

killall -s SIGQUIT monitoragent.exe
sleep 1
./monitoragent.exe config.json

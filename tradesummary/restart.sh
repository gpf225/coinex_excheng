#!/bin/bash

killall -s SIGQUIT dealrank.exe
sleep 1
./dealrank.exe config.json

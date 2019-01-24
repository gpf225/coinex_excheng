#!/bin/bash

killall -s SIGQUIT longpoll.exe
sleep 1
./longpoll.exe config.json

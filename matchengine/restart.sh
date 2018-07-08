#!/bin/bash

killall -s SIGQUIT matchengine.exe
sleep 3
./matchengine.exe config.json

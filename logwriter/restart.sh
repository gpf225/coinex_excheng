#!/bin/bash

killall -s SIGQUIT logwriter.exe
sleep 1
./logwriter.exe config.json

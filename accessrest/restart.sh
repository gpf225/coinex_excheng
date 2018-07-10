#!/bin/bash

killall -s SIGQUIT accessrest.exe
sleep 1
./accessrest.exe config.json

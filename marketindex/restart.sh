#!/bin/bash

killall -s SIGQUIT marketindex.exe
sleep 2
./marketindex.exe config.json

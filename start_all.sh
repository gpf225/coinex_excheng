#!/bin/bash
#set -x
#set -u
#command || { echo "command failed";exit 1; }


cd ./matchengine && ./start.sh
cd ../historywriter && ./start.sh 
cd ../historyreader && ./start.sh
cd ../marketprice && ./start.sh
cd ../cachecenter && ./start.sh
cd ../tradesummary && ./start.sh
cd ../accesshttp && ./start.sh

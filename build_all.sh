#!/bin/bash

cd ./network/ &&  make clean && make
cd ../utils/ &&  make clean && make
cd ../monitorcenter/ &&  make clean && make
cd ../monitoragent/ &&  make clean && make
cd ../matchengine/ &&  make clean && make
cd ../marketprice/ &&  make clean && make
cd ../historywriter/ &&  make clean && make
cd ../readhistory/ &&  make clean && make
cd ../alertcenter/ &&  make clean && make
cd ../cachecenter/ &&  make clean && make
cd ../accessrest/ &&  make clean && make
cd ../accesshttp/ &&  make clean && make
cd ../accessws/ &&  make clean && make
cd ../tradesummary/ &&  make clean && make

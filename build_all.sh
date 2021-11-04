#!/bin/bash

cd ./network/ &&  make clean && make -j4
cd ../utils/ &&  make clean && make -j4
#cd ../monitorcenter/ &&  make clean && make
#cd ../monitoragent/ &&  make clean && make
cd ../matchengine/ &&  make clean && make -j4
cd ../marketprice/ &&  make clean && make -j4
#cd ../marketindex/ &&  make clean && make
cd ../historywriter/ &&  make clean && make -j4
cd ../historyreader/ &&  make clean && make -j4
#cd ../alertcenter/ &&  make clean && make
cd ../cachecenter/ &&  make clean && make -j4
#cd ../accessrest/ &&  make clean && make
cd ../accesshttp/ &&  make clean && make -j4
#cd ../accessws/ &&  make clean && make
cd ../tradesummary/ &&  make clean && make -j4
#cd ../internalws/ &&  make clean && make

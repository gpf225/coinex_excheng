#!/bin/bash

cd ../../network/ &&  make clean && make
cd ../utils/ &&  make clean && make
cd ../cache/ &&  make clean && make
cd ../longpoll/ &&  make clean && make
cd ../accessrest/ &&  make clean && make
cd ../accessws/ &&  make clean && make
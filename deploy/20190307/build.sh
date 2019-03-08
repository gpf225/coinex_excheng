#!/bin/bash

cd ../../network/ &&  make clean && make
cd ../utils/ &&  make clean && make
cd ../accessrest/ &&  make clean && make
#!/bin/bash

cd ../../network/ &&  make clean && make
cd ../utils/ &&  make clean && make
cd ../matchengine/ &&  make clean && make
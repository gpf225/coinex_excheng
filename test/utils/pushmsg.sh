#!/bin/bash
for ((i=1000;i<10000;i++))
do
 echo "push $i" | nc 127.0.0.1 8001
 sleep 0.02
done
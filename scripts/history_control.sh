#!/bin/bash

function history_control() {
    echo "hiscontrol $1" | nc 127.0.0.1 7317
    echo "\n"
}

if [ $1 = "direct" ]; then
    echo "---write message to db directly---"
    history_control "1"
elif [ $1 = "kafka" ]; then   
    echo "---write message to db through kafka---"
    history_control "2"
elif [ $1 = "double" ]; then   
    echo "---write message to db and kafka---"
    history_control "3"
else
    echo "usage: bash history_control.sh [direct kafka double]"
fi

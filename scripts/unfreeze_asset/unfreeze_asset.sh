#!/bin/bash

ASSET="DACC"
USERS=(105 342 63 194 434 469 1 511 91 61 394 59 429 114 431 264 502 374 84 12 251)
FROZENS=(9.94 16963.83248444 1804448.64669813 2 50009.1 1 33 121 1 26499603.16511173 50022.5 2 51233.2 20316 88995.80417369 50 500 12 1 33 549.459)
LEN=${#USERS[@]}

if [  ${#USERS[@]} -ne ${#FROZENS[@]} ]; then
    echo "invalid params, USERS:${#USERS[@]} FROZENS:${#FROZENS[@]}"
    exit 0
fi

function unfreeze() {
    echo "$1" | nc 127.0.0.1 7317
    echo "\n"
}

for((i = 0; i < $LEN; i++))
do
    unfreeze "unfreeze ${USERS[i]} $ASSET ${FROZENS[i]}"
done
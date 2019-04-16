#!/bin/bash

killall -s SIGQUIT datamigrator.exe
sleep 1
./datamigrator.exe config.json datamigrator0

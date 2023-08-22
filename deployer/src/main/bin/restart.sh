#!/bin/bash

args=$@
cd $(dirname $0)
./stop.sh $args
./startup.sh $args
